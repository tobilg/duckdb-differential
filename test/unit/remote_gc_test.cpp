// Remote garbage collection below the S3 transport: condemned packs are never reused by exports,
// their verifications and locations are forgotten unless a local S3-only object still reads them,
// such objects can be copied locally, newer pack locations replace older ones, and metadata
// pruning keeps pending operations.
#include "differential_control.hpp"
#include <sqlite3.h>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
using namespace differential;
static void Check(bool result, const char *message) {
    if (!result)
        throw std::runtime_error(message);
}
static void Execute(const std::string &directory, const std::string &sql) {
    sqlite3 *db = nullptr;
    Check(sqlite3_open((directory + "/engine.sqlite").c_str(), &db) == SQLITE_OK, "open metadata");
    sqlite3_busy_timeout(db, 10000);
    Check(sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK, "execute metadata statement");
    sqlite3_close(db);
}
int main() {
    char source_pattern[] = "/tmp/differential-gc-source-XXXXXX";
    char target_pattern[] = "/tmp/differential-gc-target-XXXXXX";
    auto source_path = mkdtemp(source_pattern);
    auto target_path = mkdtemp(target_pattern);
    if (!source_path || !target_path)
        return 2;
    try {
        // Two chunk identities with their bytes, from another store.
        Store source(source_path);
        auto image = source.Open("image", true, false, false);
        std::vector<unsigned char> a(Store::CHUNK, 'a'), b(Store::CHUNK, 'b');
        source.Write(image.id, a.data(), a.size(), Store::ChunkOffset(1));
        source.Write(image.id, b.data(), b.size(), Store::ChunkOffset(2));
        auto chunks = source.RootChunks(source.Seal(image.id).id, 1, 2);
        Check(chunks.size() == 2, "source chunks");
        auto local = chunks[0].object, remote = chunks[1].object;

        auto store = std::make_shared<Store>(target_path);
        int64_t clock = 1000;
        Control control(store, [&] { return clock; });
        S3Config config;
        config.bucket = "bucket";
        config.prefix = "lake/";
        auto provider = control.EnsureS3Provider(config, {});
        std::string pool = "0b1c2d3e-4f50-4a61-9b72-c3d4e5f60718";
        auto pack_key = [&](const std::string &pack) { return "dbs/" + pool + "/packs/" + pack; };
        std::string old_pack(64, '1'), condemned_pack(64, '2'), new_pack(64, '3');

        // `local` has a local copy here; `remote` is stored only in S3 (read on demand).
        auto file = store->Open("held", true, false, false);
        store->Write(file.id, a.data(), a.size(), Store::ChunkOffset(1));
        store->Seal(file.id);
        Execute(target_path, "INSERT INTO objects(hash) VALUES('" + remote + "'); INSERT INTO "
                             "remote_objects VALUES('" + remote + "');");

        // Newer pack locations replace older ones.
        control.RecordPack(provider, pool, old_pack, 2 * Store::CHUNK, {local, remote});
        control.RecordPack(provider, pool, condemned_pack, 2 * Store::CHUNK, {local, remote});
        auto found = control.FindRemoteChunks(provider, {local, remote}, true);
        Check(found.size() == 2 && found.at(local).pack == condemned_pack && found.at(remote).slot == 1,
              "a newly recorded pack does not replace older locations");
        control.RecordRemoteVerified(provider, pack_key(condemned_pack), condemned_pack, 2 * Store::CHUNK);
        Check(control.RemoteOnlyPacks(provider) == std::set<std::string> {pack_key(condemned_pack)},
              "S3-only objects are not collection roots");

        // A record that cannot be completed yet: exports stop reusing the pack at once, and the
        // location an S3-only object reads stays.
        std::string record = "gc/condemned/00000000000000000500-" + std::string(64, 'a');
        control.RecordCondemnation(provider, record, 500, {pack_key(condemned_pack)}, false);
        Check(!control.KnownCondemnation(provider, record), "an incomplete record was marked learned");
        Check(control.Condemned(provider, pack_key(condemned_pack)), "condemned pack not recorded");
        Check(control.FindRemoteChunks(provider, {local, remote}, true).empty(), "an export may reuse a condemned pack");
        auto readable = control.FindRemoteChunks(provider, {local, remote});
        Check(readable.size() == 1 && readable.count(remote), "only S3-only objects keep condemned locations");
        Check(!control.RemoteVerified(provider, pack_key(condemned_pack), condemned_pack, 2 * Store::CHUNK),
              "a condemned object's upload verification survived");

        // The S3-only object is copied locally, then the record completes.
        auto objects = control.RemoteOnlyObjectsIn(provider, {pack_key(condemned_pack)});
        Check(objects == std::vector<std::string> {remote}, "S3-only objects of a condemned pack");
        control.MaterializeRemoteObjects({{remote, std::make_shared<const Bytes>(b)}});
        Check(control.RemoteOnlyPacks(provider).empty(), "materialized object still read from S3");
        Check(store->Object(remote)->at(0) == 'b', "materialized object bytes");
        control.RecordCondemnation(provider, record, 500, {pack_key(condemned_pack)}, true);
        Check(control.KnownCondemnation(provider, record), "completed record not learned");
        // An earlier condemnation of the same key wins.
        control.RecordCondemnation(provider, "gc/condemned/00000000000000000300-" + std::string(64, 'b'), 300,
                                   {pack_key(condemned_pack)}, true);
        Check(control.Condemnations(provider).at(pack_key(condemned_pack)) == 300, "earliest condemnation not kept");

        // A deleted pack is forgotten; a fresh upload is reusable again.
        control.ForgetRemoteObjects(provider, {pack_key(condemned_pack)});
        control.RecordPack(provider, pool, new_pack, Store::CHUNK, {local});
        Check(control.FindRemoteChunks(provider, {local}, true).at(local).pack == new_pack, "fresh pack not reusable");
        std::cout << "PASS condemned packs are never reused, S3-only objects keep their locations until copied, "
                     "earliest condemnation, forgotten deletions, replacing pack locations\n";

        // Reference cache: immutable objects' references, including "references nothing".
        Control::RemoteReferences references;
        references.index_packs["dbs/" + pool + "/indexes/a"] = {pack_key(new_pack)};
        references.index_packs["dbs/" + pool + "/indexes/holes"] = {};
        references.descriptor_pages["descriptors/d"] = {{"dbs/" + pool + "/indexes/a", 147464}};
        references.descriptor_pages["descriptors/empty"] = {};
        references.commit_descriptors["dbs/" + pool + "/snapshots/9999999998"] = "descriptors/d";
        control.StoreRemoteReferences(provider, references);
        control.StoreRemoteReferences(provider, references); // idempotent
        auto loaded = control.LoadRemoteReferences(provider);
        Check(loaded.index_packs == references.index_packs && loaded.descriptor_pages == references.descriptor_pages &&
                  loaded.commit_descriptors == references.commit_descriptors,
              "reference cache round trip");
        control.ForgetRemoteObjects(provider, {"dbs/" + pool + "/indexes/a", "descriptors/d"});
        loaded = control.LoadRemoteReferences(provider);
        Check(!loaded.index_packs.count("dbs/" + pool + "/indexes/a") && loaded.index_packs.count("dbs/" + pool + "/indexes/holes") &&
                  !loaded.descriptor_pages.count("descriptors/d") && loaded.descriptor_pages.count("descriptors/empty"),
              "deleted objects stay cached");
        std::cout << "PASS reference cache: round trip, empty references, removal with deleted objects\n";

        // Metadata pruning: completed operations before the cutoff go, newer ones stay.
        clock = 2000;
        control.CreateDatabase("old", "old-request");
        clock = 5000;
        control.CreateDatabase("new", "new-request");
        auto dry = control.PruneMetadata(3000, true);
        Check(dry.size() == 2 && dry[0].first == "operations" && dry[0].second == 1, "pruning dry run count");
        Check(control.Operations().size() == 2, "dry run deleted operations");
        control.PruneMetadata(3000, false);
        auto remaining = control.Operations();
        Check(remaining.size() == 1 && remaining[0].request == "new-request", "pruning removed the wrong operations");
        std::cout << "PASS metadata pruning deletes completed operations before the cutoff only\n";
    } catch (const std::exception &error) {
        std::cerr << error.what() << "\n";
        return 1;
    }
    std::filesystem::remove_all(source_path);
    std::filesystem::remove_all(target_path);
    return 0;
}

#include "differential_control.hpp"
#include <filesystem>
#include <iostream>
#include <random>
#include <set>
#include <unistd.h>

using namespace differential;
void Check(bool ok) {
    if (!ok)
        throw std::runtime_error("Ancestry invariant failed");
}
int main() {
    char pattern[] = "/tmp/differential-ancestry-XXXXXX";
    auto folder = mkdtemp(pattern);
    if (!folder)
        return 2;
    try {
        auto store = std::make_shared<Store>(folder);
        Control c(store);
        auto database = c.CreateDatabase("graph", "").database;
        auto lease = c.Acquire(c.GetBranch(database, "main"));
        auto file = store->Open(lease->branch.FileName(), true, false, false);
        store->Write(file.id, "graph", 5, 0);
        auto root = store->Seal(file.id);
        c.PublishSnapshot(c.BeginSnapshot(lease->branch, {}, ""), lease->branch, root, 1);
        // These are metadata graph fixtures, not SQL database images. Use the same
        // root for every node to ensure content equality never establishes ancestry.
        std::vector<std::set<int64_t>> ancestors(2);
        ancestors[1].insert(1);
        std::mt19937 rng(43619);
        auto branch = [](int64_t n) { return n == 1 ? std::string("main") : "n" + std::to_string(n); };
        for (int64_t n = 2; n <= 64; n++) {
            int64_t left = 1 + rng() % (n - 1), right = 1 + rng() % (n - 1);
            auto op = c.BeginMerge(database, 1, left, right, branch(n), "");
            Check(c.PublishMerge(op, root, "").snapshot == n);
            auto all = ancestors[left];
            all.insert(ancestors[right].begin(), ancestors[right].end());
            all.insert(n);
            ancestors.push_back(std::move(all));
        }
        int64_t pairs = 0, ambiguous = 0;
        for (int64_t left = 1; left <= 64; left++) {
            for (int64_t right = left + 1; right <= 64; right++) {
                // Independent transitive-set oracle: compare every common ancestor
                // against every other candidate, rather than the resolver's edge walk.
                std::set<int64_t> common, best;
                for (auto node : ancestors[left])
                    if (ancestors[right].count(node))
                        common.insert(node);
                for (auto node : common) {
                    bool dominated = false;
                    for (auto other : common)
                        dominated |= other != node && ancestors[other].count(node);
                    if (!dominated)
                        best.insert(node);
                }
                try {
                    auto selected = c.ResolveMergeBranches(database, branch(left), branch(right), "", "");
                    Check(best.size() == 1 && selected.base == *best.begin());
                } catch (const std::exception &e) {
                    Check(best.size() > 1 && std::string(e.what()).find("merge_base_ambiguous") != std::string::npos);
                    ambiguous++;
                }
                auto inspection = c.InspectMergeAncestry(database, left, right, 100000);
                std::set<int64_t> inspected;
                for (auto &node : inspection.nodes) {
                    Check(node.known && node.readable && !node.retired);
                    Check(node.reach == (unsigned(ancestors[left].count(node.snapshot)) |
                                         (unsigned(ancestors[right].count(node.snapshot)) << 1)));
                    if (node.candidate) inspected.insert(node.snapshot);
                }
                Check(inspected == best);
                Check(inspection.state == (best.size() == 1 ? "unique" : "ambiguous"));
                Check(inspection.selected == (best.size() == 1 ? *best.begin() : 0));
                pairs++;
            }
        }
        Check(ambiguous > 0);
        auto operations = c.Operations().size();
        int checks = 0;
        try {
            c.ResolveMergeBranches(database, "n64", "n63", "cancelled", "cancelled", 100000, [&] {
                if (++checks == 3)
                    throw std::runtime_error("cancel ancestry");
            });
            Check(false);
        } catch (const std::exception &e) {
            Check(std::string(e.what()) == "cancel ancestry");
        }
        Check(checks == 3 && c.Operations().size() == operations);
        auto orphan = c.Acquire(c.CreateEmptyBranch(database, "orphan"));
        c.PublishSnapshot(c.BeginSnapshot(orphan->branch, {}, ""), orphan->branch, root, 1);
        try {
            c.ResolveMergeBranches(database, "main", "orphan", "", "");
            Check(false);
        } catch (const std::exception &e) {
            Check(std::string(e.what()).find("No common ancestor") != std::string::npos);
        }
        auto disconnected = c.InspectMergeAncestry(database, 1, 65, 100000);
        Check(disconnected.state == "unrelated" && !disconnected.selected && disconnected.nodes.size() == 2);
        std::cout << "PASS " << pairs << " graph pairs against independent oracle (" << ambiguous
                  << " ambiguous), equal roots, deduplicated parents, disconnected roots and interrupted selection\n";
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        std::filesystem::remove_all(folder);
        return 1;
    }
    std::filesystem::remove_all(folder);
}

#pragma once

#include <algorithm>
#include <functional>
#include <iterator>
#include <list>
#include <string>
#include <unordered_map>

/**
 * pinnedの考慮
 * 実際の解放処理
 */
class ARCReplacement {

 private:
    // 多分pageid, filename?
    int t1_target_ = 0;
    int total_frame_count_;

    enum class ArcList {
        T1,
        T2,
        B1,
        B2,
    };

    struct PageKey {
        std::string file_identifier;
        int page_id;

        bool operator==(const PageKey& other) const {
            return file_identifier == other.file_identifier &&
                   page_id == other.page_id;
        }
    };

    struct PageKeyHash {
        size_t operator()(const PageKey& key) const {
            const size_t file_hash =
                std::hash<std::string>{}(key.file_identifier);
            const size_t page_hash = std::hash<int>{}(key.page_id);
            return file_hash ^ (page_hash << 1);
        }
    };

    struct ArcEntry {
        ArcList list;
        std::list<PageKey>::iterator it;
    };
    
    std::list<PageKey> t1_;
    std::list<PageKey> t2_;
    std::list<PageKey> b1_;
    std::list<PageKey> b2_;
    std::unordered_map<PageKey, ArcEntry, PageKeyHash> entries_;

    void moveEntry(ArcEntry& entry, std::list<PageKey>& from,
                   std::list<PageKey>& to, ArcList destination) {
        to.splice(to.end(), from, entry.it);
        entry.list = destination;
    }

    void moveLru(std::list<PageKey>& from, std::list<PageKey>& to,
                 ArcList destination) {
        ArcEntry& entry = entries_.at(from.front());
        moveEntry(entry, from, to, destination);
    }

    void removeLru(std::list<PageKey>& list) {
        const PageKey key = list.front();
        list.pop_front();
        entries_.erase(key);
    }

    void trimGhostListForNewPage() {
        const size_t capacity = static_cast<size_t>(total_frame_count_);
        const size_t recent_size = t1_.size() + b1_.size();
        const size_t total_size =
            recent_size + t2_.size() + b2_.size();

        if (recent_size == capacity) {
            if (!b1_.empty()) {
                removeLru(b1_);
            }
        } else if (recent_size < capacity &&
                   total_size >= 2 * capacity) {
            if (!b2_.empty()) {
                removeLru(b2_);
            }
        }
    }

    void ensureFreeFrameExists(){
        if(t1_.size() + t2_.size() ==
           static_cast<size_t>(total_frame_count_)){
            const bool evict_from_t1 =
                !t1_.empty() &&
                (t2_.empty() ||
                 t1_.size() >= static_cast<size_t>(t1_target_));
            if(evict_from_t1){
                moveLru(t1_, b1_, ArcList::B1);
            }else if(!t2_.empty()){
                moveLru(t2_, b2_, ArcList::B2);
            }
        }
    }

  public:

    explicit ARCReplacement(int total_frame_count)
        : total_frame_count_(total_frame_count) {}

    void read_page(const std::string& file_identifier, int page_id){
        const PageKey key{file_identifier, page_id};
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            ArcEntry& entry = it->second;
            if(entry.list == ArcList::T1){
                moveEntry(entry, t1_, t2_, ArcList::T2);
            }else if(entry.list == ArcList::T2){
                moveEntry(entry, t2_, t2_, ArcList::T2);
            }else if(entry.list == ArcList::B1){
                const int adjustment = std::max(
                    1, static_cast<int>(b2_.size() / b1_.size()));
                t1_target_ = std::min(
                    total_frame_count_, t1_target_ + adjustment);
                ensureFreeFrameExists();
                moveEntry(entry, b1_, t2_, ArcList::T2);
            }else if(entry.list == ArcList::B2){
                const int adjustment = std::max(
                    1, static_cast<int>(b1_.size() / b2_.size()));
                t1_target_ = std::max(0, t1_target_ - adjustment);
                ensureFreeFrameExists();
                moveEntry(entry, b2_, t2_, ArcList::T2);
            }

        }else{
            trimGhostListForNewPage();
            ensureFreeFrameExists();
            // add page to T1.
            t1_.push_back(key);
            ArcEntry entry;
            entry.list = ArcList::T1;
            entry.it = std::prev(t1_.end());
            entries_[key] = entry;
        }
    }
};

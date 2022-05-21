//
// Created by Rainy Memory on 2021/3/18.
//

#ifndef TICKETSYSTEM_AUTOMATA_LRUCACHEMEMORYPOOL_H
#define TICKETSYSTEM_AUTOMATA_LRUCACHEMEMORYPOOL_H

#include <iostream>
//#include "HashMap.h"

//#define useCache

/*
 * Current problem: The existence of cache make write or update of file
 * might not be executed with timestamp increasing order, say, we have an
 * update operation that execute in [1] and leave cache in [100], in the
 * same time another operation that execute in [10] leave cache in [20],
 * which make actually update of file executed as [10], [1]. This make
 * rollback need to traverse whole index file, which may be very slow.
 * A naive fix: do not use cache! (currently using this method)
 * TODO: Maybe we could bruteforce traverse whole index file.
 * */

using std::string;

namespace RainyMemory {
    class MemoryPoolTimeStamp {
    public:
        static int timeStamp;
    };
    
    template <class T, class extraMessage = int>
    class LRUCacheMemoryPool {
        /*
         * class LRUCacheMemoryPool
         * --------------------------------------------------------
         * A class implements memory pool which has built-in cache
         * strategy to storage and quick access data.
         * This class offer single type data's storage, update and
         * delete in file, and LRU Cache to accelerate accession.
         * Also, this class support deleted data's space reclamation,
         * and an extra (not same type of stored data) message storage.
         *
         */
    private:
        enum LogType {
            WRITE, UPDATE, ERASE, UPDATE_EXTRA
        };
        
        class MemoryPoolRollbackLog {
            /*
             * class MemoryPoolRollbackLog
             * --------------------------------------------------------
             * A class implements log for rollback. After each operation
             * that changed the file (write, update, erase), this class
             * will log a reverse operation with time stamp. While rollback,
             * we only need to read these reverse operations in timestamp
             * decreasing order (i.e. from log file's back to front), and
             * execute them.
             * This class maintain two files, an index file storage each log's
             * start offset and its size, and a log file to storage log.
             *
             * index file: [logNum] [offset, size]* // maintain [logNum] to support delete
             * log file:   [indexNum] [log]*        // each log's length might verify
             *
             * The following pseudocode describe how to read from back to front:
             *     n <- logNum
             *     while (true) {
             *         idx <- read nth index;
             *         entry <- read from idx.offset with idx.size in log file;
             *         if (entry.timeStamp < requiredTimeStamp) break;
             *         n--, execute entry.reverseOperation;
             *     }
             *
             */
        private:
            LRUCacheMemoryPool * nest;
            const string logName, indexName;
            int indexCnt, logFp;
            
            template <class U>
            void fw(FILE * f, int offset, const U &val) {
                fseek(f, offset, SEEK_SET);
                fwrite(reinterpret_cast<const char *>(&val), sizeof(U), 1, f);
            }
            
            template <class U>
            void fr(FILE * f, int offset, U &val) {
                fseek(f, offset, SEEK_SET);
                fread(reinterpret_cast<char *>(&val), sizeof(U), 1, f);
            }
        
        public:
            struct WriteEntry {
                int offset = -1;
                T value;
                
                WriteEntry() = default;
                
                WriteEntry(int o, const T &v) : offset(o), value(v) {}
            };
            
            struct UpdateEntry {
                int offset = -1;
                T value;
                
                UpdateEntry() = default;
                
                UpdateEntry(int o, const T &v) : offset(o), value(v) {}
            };
            
            struct EraseEntry {
                int offset = -1;
                
                EraseEntry() = default;
    
                explicit EraseEntry(int o) : offset(o) {}
            };
            
            struct UpdateExtraEntry {
                extraMessage value;
                
                UpdateExtraEntry() = default;
                
                explicit UpdateExtraEntry(const extraMessage &v) : value(v) {}
            };
            
            struct IndexEntry {
                int timeStamp = -1;
                int offset = -1;
                LogType type;
                
                IndexEntry() = default;
                
                IndexEntry(int t, int o, LogType ty) : timeStamp(t), offset(o), type(ty) {}
            };
            
            MemoryPoolRollbackLog(const string &name, LRUCacheMemoryPool * n) : logName("log_" + name), indexName("index_" + name), nest(n) {
                auto f = fopen(logName.c_str(), "rb");
                if (f == nullptr) {
                    f = fopen(logName.c_str(), "wb+");
                    fw(f, 0, logFp = sizeof(int));
                    fclose(f);
                    f = fopen(indexName.c_str(), "wb+");
                    fw(f, 0, indexCnt = 0);
                    fclose(f);
                } else {
                    fr(f, 0, logFp);
                    fclose(f);
                    f = fopen(indexName.c_str(), "rb");
                    fr(f, 0, indexCnt);
                    fclose(f);
                }
            }
            
            void logWrite(int timeStamp, int index, const T &value) {
                auto fl = fopen(logName.c_str(), "rb+");
                auto fi = fopen(indexName.c_str(), "rb+");
                fw(fi, indexCnt * sizeof(IndexEntry) + sizeof(int), IndexEntry(timeStamp, logFp, WRITE));
                fw(fi, 0, ++indexCnt);
                fw(fl, logFp, WriteEntry(index, value));
                fw(fl, 0, logFp += sizeof(WriteEntry));
                fclose(fi), fclose(fl);
            }
            
            void logUpdate(int timeStamp, int index, const T &value) {
                auto fl = fopen(logName.c_str(), "rb+");
                auto fi = fopen(indexName.c_str(), "rb+");
                fw(fi, indexCnt * sizeof(IndexEntry) + sizeof(int), IndexEntry(timeStamp, logFp, UPDATE));
                fw(fi, 0, ++indexCnt);
                fw(fl, logFp, UpdateEntry(index, value));
                fw(fl, 0, logFp += sizeof(UpdateEntry));
                fclose(fi), fclose(fl);
            }
            
            void logErase(int timeStamp, int index) {
                auto fl = fopen(logName.c_str(), "rb+");
                auto fi = fopen(indexName.c_str(), "rb+");
                fw(fi, indexCnt * sizeof(IndexEntry) + sizeof(int), IndexEntry(timeStamp, logFp, ERASE));
                fw(fi, 0, ++indexCnt);
                fw(fl, logFp, EraseEntry(index));
                fw(fl, 0, logFp += sizeof(EraseEntry));
                fclose(fi), fclose(fl);
            }
            
            void logUpdateExtra(int timeStamp, const extraMessage &value) {
                auto fl = fopen(logName.c_str(), "rb+");
                auto fi = fopen(indexName.c_str(), "rb+");
                fw(fi, indexCnt * sizeof(IndexEntry) + sizeof(int), IndexEntry(timeStamp, logFp, UPDATE_EXTRA));
                fw(fi, 0, ++indexCnt);
                fw(fl, logFp, UpdateExtraEntry(value));
                fw(fl, 0, logFp += sizeof(UpdateExtraEntry));
                fclose(fi), fclose(fl);
            }
            
            void rollback(int timeStamp) {
                auto fl = fopen(logName.c_str(), "rb+");
                auto fi = fopen(indexName.c_str(), "rb+");
                while (true) {
                    IndexEntry idx;
                    fr(fi, (indexCnt - 1) * sizeof(IndexEntry) + sizeof(int), idx);
                    if (idx.timeStamp <= timeStamp || indexCnt == 0) {
                        fw(fi, 0, indexCnt);
                        fw(fl, 0, logFp);
                        break;
                    }
                    indexCnt--;
                    if (idx.type == WRITE) {
                        std::cerr << "unexpected write log" << std::endl;
                        WriteEntry ew;
                        fr(fl, logFp -= sizeof(WriteEntry), ew);
                        nest->writeInFileOffset(ew.offset, ew.value);
                    } else if (idx.type == UPDATE) {
                        UpdateEntry eu;
                        fr(fl, logFp -= sizeof(UpdateEntry), eu);
                        nest->updateInFile(eu.offset, eu.value, false);
                    } else if (idx.type == ERASE) {
                        std::cerr << "unexpected erase log" << std::endl;
                        EraseEntry ee;
                        fr(fl, logFp -= sizeof(EraseEntry), ee);
                        nest->eraseInFile(ee.offset, false);
                    } else if (idx.type == UPDATE_EXTRA) {
                        UpdateExtraEntry eue;
                        fr(fl, logFp -= sizeof(UpdateExtraEntry), eue);
                        nest->updateExtraMessage(eue.value, false);
                    }
                }
                fclose(fi), fclose(fl);
            }
        };

#ifdef useCache
        
        class DoublyLinkedList {
        public:
            class Node {
            public:
                int key = -1;
                T * value = nullptr;
                Node * pre = nullptr;
                Node * next = nullptr;
                bool dirtyBit = false;
                
                Node() = default;
                
                Node(int k, const T &v, Node * p = nullptr, Node * n = nullptr) : key(k), value(new T(v)), pre(p), next(n) {}
                
                ~Node() {
                    delete value;
                }
            };
            
            Node * head = nullptr;
            Node * tail = nullptr;
            int listSize = 0;
            int capacity = 0;
            
            explicit DoublyLinkedList(int _capacity) : capacity(_capacity) {
                head = new Node(), tail = new Node();
                head->next = tail, tail->pre = head;
            }
            
            ~DoublyLinkedList() {
                Node * temp = head;
                while (head != nullptr) {
                    head = head->next;
                    delete temp;
                    temp = head;
                }
            }
            
            void clear() {
                listSize = 0;
                Node * temp = head;
                while (head != nullptr) {
                    head = head->next;
                    delete temp;
                    temp = head;
                }
                head = new Node(), tail = new Node();
                head->next = tail, tail->pre = head;
            }
            
            void push_front(Node * n) {
                head->next->pre = n;
                n->next = head->next;
                head->next = n;
                n->pre = head;
                listSize++;
            }
            
            void to_front(Node * n) {
                n->pre->next = n->next;
                n->next->pre = n->pre;
                listSize--;
                push_front(n);
            }
            
            Node * pop_back() {
                Node * target = tail->pre;
                target->pre->next = tail;
                tail->pre = tail->pre->pre;
                listSize--;
                return target;
            }
            
            void erase(Node * n) {
                n->pre->next = n->next;
                n->next->pre = n->pre;
                listSize--;
                delete n;
            }
            
            bool full() const {
                return listSize == capacity;
            }
        };
        
        using node_t = typename DoublyLinkedList::Node;
        
#endif
    
    private:
        MemoryPoolRollbackLog log;
        const string filename;
        FILE * file;

#ifdef useCache
        
        HashMap<int, node_t *> hashmap;
        DoublyLinkedList cache;
        
        bool existInCache(int key) {
            return hashmap.containsKey(key);
        }
        
        void discardLRU() {
            node_t * target = cache.pop_back();
            hashmap.erase(target->key);
            if (target->dirtyBit)updateInFile(target->key, *target->value);
            delete target;
        }
        
        void eraseInCache(int key) {
            cache.erase(hashmap[key]);
            hashmap.erase(key);
        }
        
        void putInCache(int key, const T &o) {
            if (existInCache(key)) {
                cache.to_front(hashmap[key]);
                *hashmap[key]->value = o;
                return;
            }
            auto newNode = new node_t(key, o);
            if (cache.full())discardLRU();
            cache.push_front(newNode);
            hashmap[key] = newNode;
        }

#endif
        
        int writeInFile(const T &o, bool doLog = true) {
            int offset;
            file = fopen(filename.c_str(), "rb+");
            fseek(file, 0, SEEK_END);
            offset = ftell(file);
            fwrite(reinterpret_cast<const char *>(&o), sizeof(T), 1, file);
            fclose(file);
//            if (doLog) log.logErase(MemoryPoolTimeStamp::timeStamp, offset);
            return offset;
        }
        
        // only for log reverse
        int writeInFileOffset(int offset, const T &o) {
            file = fopen(filename.c_str(), "rb+");
            fseek(file, offset, SEEK_SET);
            fwrite(reinterpret_cast<const char *>(&o), sizeof(T), 1, file);
            fclose(file);
            return offset;
        }
        
        T readInFile(int offset) {
            file = fopen(filename.c_str(), "rb");
            T temp;
            fseek(file, offset, SEEK_SET);
            fread(reinterpret_cast<char *>(&temp), sizeof(T), 1, file);
            fclose(file);
            return temp;
        }
        
        void updateInFile(int offset, const T &o, bool doLog = true) {
            file = fopen(filename.c_str(), "rb+");
            T now;
            fseek(file, offset, SEEK_SET);
            fread(reinterpret_cast<char *>(&now), sizeof(T), 1, file);
            fseek(file, offset, SEEK_SET);
            fwrite(reinterpret_cast<const char *>(&o), sizeof(T), 1, file);
            fclose(file);
            if (doLog) log.logUpdate(MemoryPoolTimeStamp::timeStamp, offset, now);
        }
        
        void eraseInFile(int offset, bool doLog = true) {
            file = fopen(filename.c_str(), "rb+");
            fseek(file, offset, SEEK_SET);
            T temp;
            fread(reinterpret_cast<char *>(&temp), sizeof(T), 1, file);
            fclose(file);
//            if (doLog) log.logWrite(MemoryPoolTimeStamp::timeStamp, offset, temp);
        }

#ifdef useCache
        
        void clearCache() {
            node_t * now = cache.head->next;
            while (now != cache.tail) {
                if (now->dirtyBit)updateInFile(now->key, *now->value, true, timestamp);
                now = now->next;
            }
            hashmap.clear();
            cache.clear();
        }

#endif
    
    public:
        explicit LRUCacheMemoryPool(const string &_filename, const extraMessage &ex = extraMessage {}, int _capacity = 100) : filename(_filename),
#ifdef useCache
                                                                                                                              cache(_capacity), hashmap(),
#endif
                                                                                                                              log(_filename, this) {
            file = fopen(filename.c_str(), "rb");
            if (file == nullptr) {
                file = fopen(filename.c_str(), "wb");
                extraMessage temp(ex);
                fwrite(reinterpret_cast<const char *>(&temp), sizeof(extraMessage), 1, file);
            }
            fclose(file);
        }
        
        ~LRUCacheMemoryPool() {
#ifdef useCache
            node_t * now = cache.head->next;
            while (now != cache.tail) {
                if (now->dirtyBit)updateInFile(now->key, *now->value);
                now = now->next;
            }
#endif
        }
        
        T read(int offset) {
#ifdef useCache
            T temp = existInCache(offset) ? *hashmap[offset]->value : readInFile(offset);
            putInCache(offset, temp);
            return temp;
#else
            return readInFile(offset);
#endif
        }
        
        int write(const T &o) {
#ifdef useCache
            int offset = writeInFile(o);
            putInCache(offset, o);
            return offset;
#else
            return writeInFile(o);
#endif
        }
        
        void update(const T &o, int offset) {
#ifdef useCache
            putInCache(offset, o);
            hashmap[offset]->dirtyBit = true;
#else
            updateInFile(offset, o);
#endif
        }
        
        void erase(int offset) {
#ifdef useCache
            if (existInCache(offset))eraseInCache(offset);
#endif
            eraseInFile(offset);
        }
        
        void clear(extraMessage ex = extraMessage {}) {
#ifdef useCache
            clearCache();
#endif
            file = fopen(filename.c_str(), "wb+");
            fclose(file);
            extraMessage temp(ex);
            file = fopen(filename.c_str(), "rb+");
            fseek(file, 0, SEEK_SET);
            fwrite(reinterpret_cast<const char *>(&temp), sizeof(extraMessage), 1, file);
            fclose(file);
        }
        
        extraMessage readExtraMessage() {
            file = fopen(filename.c_str(), "rb+");
            fseek(file, 0, SEEK_SET);
            extraMessage temp;
            fread(reinterpret_cast<char *>(&temp), sizeof(extraMessage), 1, file);
            fclose(file);
            return temp;
        }
        
        void updateExtraMessage(const extraMessage &o, bool doLog = true) {
            file = fopen(filename.c_str(), "rb+");
            extraMessage now;
            fseek(file, 0, SEEK_SET);
            fread(reinterpret_cast<char *>(&now), sizeof(extraMessage), 1, file);
            fseek(file, 0, SEEK_SET);
            fwrite(reinterpret_cast<const char *>(&o), sizeof(extraMessage), 1, file);
            fclose(file);
            if (doLog) log.logUpdateExtra(MemoryPoolTimeStamp::timeStamp, now);
        }
        
        int tellWritePoint() {
            file = fopen(filename.c_str(), "rb+");
            fseek(file, 0, SEEK_END);
            int tempWritePoint = ftell(file);
            fclose(file);
            return tempWritePoint;
        }
        
        void rollback(int timeStamp) {
#ifdef useCache
            clearCache();
#endif
            log.rollback(timeStamp);
        }
    };
}

#endif //TICKETSYSTEM_AUTOMATA_LRUCACHEMEMORYPOOL_H

#pragma once

#include <cstdint>

#include "wal_record.h"

class WAL
{
public:
    // ファイル出力するメソッド
    // bufferpoolのページのflush済みlsnを更新するメソッド
    void flush();

    // これだど、flushする前に呼び出し側が終了してWALRecordを手放す可能性がある？
    void write(const WALRecord& record);
};

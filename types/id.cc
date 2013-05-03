/* id.cc
   Jeremy Barnes, 17 February 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#include "id.h"
#include "jml/arch/bit_range_ops.h"
#include "jml/arch/format.h"
#include "jml/arch/exception.h"
#include "jml/db/persistent.h"
#include "jml/utils/exc_assert.h"
#include "soa/jsoncpp/value.h"

using namespace ML;
using namespace std;


namespace Datacratic {


/*****************************************************************************/
/* ID                                                                        */
/*****************************************************************************/

static const signed char hexToDecLookups[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
};

JML_ALWAYS_INLINE int hexToDec(unsigned c) JML_CONST_FN;

JML_ALWAYS_INLINE int hexToDec(unsigned c)
{
    //cerr << "c = " << (char)c
    //     << " index " << (c & 0x7f) << " value = "
    //     << (int)lookups[c & 0x7f] << endl;
    int mask = (c <= 0x7f);
    return hexToDecLookups[c & 0x7f] * mask - 1 + mask;
}

// Not quite base64... these are re-arranged so that their ASCII order
// corresponds to their integer value so they sort uniformly
static const signed char base64ToDecLookups[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  0, -1, -1, -1,  1,
     2,  3,  4,  5,  6,  7,  8,  9, 10, 11, -1, -1, -1, -1, -1, -1,

    -1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, -1, -1, -1, -1, -1,
    -1, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, -1, -1, -1, -1, -1,
};

JML_ALWAYS_INLINE int base64ToDec(unsigned c)
{
    int mask = (c <= 0x7f);
    return base64ToDecLookups[c & 0x7f] * mask - 1 + mask;
}

inline int hexToDec3(int c)
{
    int d = c & 0x1f;
    int i = (c & 0x60) >> 5;
    d += (i == 1) * -16;
    d += (i >= 2) * 9;
    bool h = __builtin_isxdigit(c);
    return h * d - (!h);
}

inline int hexToDec2(int c)
{
    int v;

    if (c >= '0' && c <= '9')
        v = c - '0';
    else if (c >= 'a' && c <= 'f')
        v = c + 10 - 'a';
    else if (c >= 'A' && c <= 'F')
        v = c + 10 - 'A';
    else
        v = -1;

    return v;
}

void
Id::
parse(const std::string & value, Type type)
{
    Id r;

    auto finish = [&] ()
        {
            //if (r.toString() != value)
            //    throw ML::Exception("Id::parse() modified an Id: input " + value
            //                        + " output " + r.toString());
            if (r.type != type && type != UNKNOWN)
                throw ML::Exception("Id::parse() changed type from %d to %d parsing %s",
                                    r.type, type, value.c_str());

            *this = r;
        };
    
    if ((type == UNKNOWN || type == NONE) && value.empty()) {
        r.type = NONE;
        r.val1 = r.val2 = 0;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == NULLID) && value.length() == 4 && value == "null") {
        //throw ML::Exception("null id");
        r.type = NULLID;
        r.val1 = r.val2 = 0;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == BIGDEC) && (value.size() == 1 && value[0] == '0')) {
        r.type = BIGDEC;
        r.val = 0;
        finish();
        return;
    }

    while ((type == UNKNOWN ||type == UUID) && value.length() == 36) {
        // not really a while...
        // Try a uuid
        // AGID: --> 0828398c-5965-11e0-84c8-0026b937c8e1

        if (value[8] != '-') break;
        if (value[13] != '-') break;
        if (value[18] != '-') break;
        if (value[23] != '-') break;

        unsigned f1;
        short f2, f3, f4;
        unsigned long long f5;

        const char * p = value.c_str();
        bool failed = false;

        auto scanRange = [&] (int start, int len) -> unsigned long long
            {
                unsigned long long val = 0;
                for (unsigned i = start;  i != start + len;  ++i) {
                    int c = p[i];
                    int v = hexToDec(c);
                    if (v == -1) {
                        failed = true;
                        return val;
                    }
                    val = (val << 4) + v;
                }
                return val;
            };

        f1 = scanRange(0, 8);
        f2 = scanRange(9, 4);
        f3 = scanRange(14, 4);
        if (failed) break;
        f4 = scanRange(19, 4);
        f5 = scanRange(24, 12);
        if (failed) break;

        r.type = UUID;
        r.f1 = f1;  r.f2 = f2;  r.f3 = f3;  r.f4 = f4;  r.f5 = f5;
        //r.val1 = ((uint64_t)f1 << 32) | ((uint64_t)f2 << 16) | f3;
        //r.val2 = ((uint64_t)f4 << 48) | f5;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == GOOG128)
        && value.length() == 26 && value[0] == 'C' && value[1] == 'A'
        && value[2] == 'E' && value[3] == 'S' && value[4] == 'E') {

        // Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA

        __uint128_t res = 0;

        auto b64Decode = [] (int c) -> int
            {
                if (c >= '0' && c <= '9')
                    return c - '0';
                else if (c >= 'A' && c <= 'Z')
                    return 10 + c - 'A';
                else if (c >= 'a' && c <= 'z')
                    return 36 + c - 'a';
                else if (c == '-')
                    return 62;
                else if (c == '_')
                    return 63;
                else return -1;
            };

        bool error = false;
        for (unsigned i = 5;  i < 26 && !error;  ++i) {
            int v = b64Decode(value[i]);
            if (v == -1) error = true;
            res = (res << 6) | v;
        }

        if (!error) {
            r.type = GOOG128;
            r.val = res;
            finish();
            return;
        }
    }

    if ((type == UNKNOWN || type == BIGDEC)
        && value[0] != '0' && value.length() < 32 /* TODO: better condition */) {
        // Try a big integer
        //ANID: --> 7394206091425759590
        __uint128_t res = 0;
        bool error = false;
        for (unsigned i = 0;  i < value.size();  ++i) {
            if (!isdigit(value[i])) error = true;
            res = 10 * res + value[i] - '0';
        }

        if (!error) {
            r.type = BIGDEC;
            r.val = res;
            finish();
            return;
        }
    }

    if ((type == UNKNOWN || type == BASE64_96) && value.length() == 16) {
        auto scanRange = [&] (const char * p, size_t l) -> int64_t
            {
                uint64_t res = 0;
                for (unsigned i = 0;  i < l;  ++i) {
                    int c = base64ToDec(p[i]);
                    if (c == -1) return -1;
                    res = res << 6 | c;
                }
                return res;
            };
        
        int64_t high = scanRange(value.c_str(), 8);
        int64_t low  = scanRange(value.c_str() + 8, 8);

        if (low != -1 && high != -1) {
            __int128_t val = high;
            val <<= 48;
            val |= low;
            
            r.type = BASE64_96;
            r.val = val;
            finish();
            return;
        }
    }   

    //cerr << "value.length() = " << value.length()
    //     << " value = " << value << " type = " << (int)type << endl;

    while ((type == UNKNOWN || type == HEX128LC) && value.length() == 32) {
        uint64_t high, low;

        const char * p = value.c_str();
        bool failed = false;

        auto scanRange = [&] (int start, int len) -> unsigned long long
            {
                uint64_t val = 0;
                for (unsigned i = start;  i != start + len;  ++i) {
                    int c = p[i];
                    int v = hexToDec(c);

                    //cerr << "c = " << c << " " << p[i] << " v = " << v << endl;

                    if (v == -1) {
                        failed = true;
                        return val;
                    }
                    val = (val << 4) + v;
                }

                //cerr << "val = " << val << " failed = " << failed << endl;

                return val;
            };

        high = scanRange(0, 16);
        if (failed)
            break;
        low = scanRange(16, 16);
        if (failed)
            break;

        r.type = HEX128LC;
        r.val1 = high;
        r.val2 = low;
        finish();
        return;
    }

    // Fall back to string
    r.type = STR;
    r.len = value.size();
    char * s = new char[r.len];
    r.str = s;
    r.ownstr = true;
    std::copy(value.c_str(), value.c_str() + value.size(), s);
    finish();
    return;
}

const Id &
Id::
compoundId1() const
{
    ExcAssertEqual(type, COMPOUND2);
    return *cmp1;
}

const Id &
Id::
compoundId2() const
{
    ExcAssertEqual(type, COMPOUND2);
    return *cmp2;
}
    
    
std::string
Id::
toString() const
{
    switch (type) {
    case NONE:
        return "";
    case NULLID:
        return "null";
    case UUID:
        // AGID: --> 0828398c-5965-11e0-84c8-0026b937c8e1
        return ML::format("%08lx-%04x-%04x-%04x-%012llx",
                          (unsigned)f1, (unsigned)f2, (unsigned)f3, (unsigned)f4,
                          (unsigned long long)f5);
    case GOOG128: {
        // Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA
        string result = "CAESE                     ";

        auto b64Encode = [] (unsigned i) -> int
            {
                if (i < 10) return i + '0';
                if (i < 36) return i - 10 + 'A';
                if (i < 62) return i - 36 + 'a';
                if (i == 62) return '-';
                if (i == 63) return '_';
                throw ML::Exception("bad goog base64 char");
            };

        __uint128_t v = val;
        for (unsigned i = 0;  i < 21;  ++i) {
            result[25 - i] = b64Encode(v & 63);  v = v >> 6;
        }
        return result;
    }
    case BIGDEC: {
        __uint128_t v = val;
        if (v == 0) return "0";
        if (val2 == 0) {
            return ML::format("%lld", v);
        }
        string result;
        result.reserve(32);
        while (v) {
            int c = v % 10;
            v /= 10;
            result += c + '0';
        }
        std::reverse(result.begin(), result.end());
        return result;
    }
    case BASE64_96: {
        string result = "                ";

        auto b64Encode = [] (unsigned i) -> int
            {
                if (i == 0) return '+';
                if (i == 1) return '/';
                if (i < 12) return '0' + i - 2;
                if (i < 38) return 'A' + i - 12;
                if (i < 64) return 'a' + i - 38;
                throw ML::Exception("bad base64 char");
            };

        __uint128_t v = val;
        for (unsigned i = 0;  i < 16;  ++i) {
            result[15 - i] = b64Encode(v & 63);  v = v >> 6;
        }
        return result;
    }
    case HEX128LC: {
        return ML::format("%016llx%016llx",
                          (unsigned long long)val1,
                          (unsigned long long)val2);
    }
    case COMPOUND2:
        return compoundId1().toString() + ":" + compoundId2().toString();
    case STR:
        return std::string(str, str + len);
    default:
        throw ML::Exception("unknown ID type");
    }
}

bool
Id::
complexEqual(const Id & other) const
{
    if (type == STR)
        return len == other.len && (str == other.str || std::equal(str, str + len, other.str));
    else if (type == COMPOUND2) {
        return compoundId1() == other.compoundId1()
            && compoundId2() == other.compoundId2();
    }
    else throw ML::Exception("unknown Id type");
}

bool
Id::
complexLess(const Id & other) const
{
    if (type == STR)
        return std::lexicographical_compare(str, str + len,
                                            other.str, other.str + other.len);
    else if (type == COMPOUND2) {
        return ML::less_all(compoundId1(), other.compoundId1(),
                            compoundId2(), other.compoundId2());
    }
    else throw ML::Exception("unknown Id type");
}

uint64_t
Id::
complexHash() const
{
    if (type == STR)
        return CityHash64(str, len);
    else if (type == COMPOUND2) {
        return Hash128to64(make_pair(compoundId1().hash(),
                                     compoundId2().hash()));
    }
    //else if (type == CUSTOM)
    //    return controlFn(CF_HASH, data);
    else throw ML::Exception("unknown Id type");
}

void
Id::
complexDestroy()
{
    if (type < STR) return;
    if (type == STR) {
        if (ownstr) delete[] str;
        str = 0;
        ownstr = false;
    }
    else if (type == COMPOUND2) {
        delete cmp1;
        delete cmp2;
        cmp1 = cmp2 = 0;
    }
    //else if (type == CUSTOM)
    //    controlFn(CF_DESTROY, data);
    else throw ML::Exception("unknown Id type");
}

void
Id::
complexFinishCopy()
{
    if (type == STR) {
        if (!ownstr) return;
        const char * oldStr = str;
        char * s = new char[len];
        str = s;
        std::copy(oldStr, oldStr + len, s);
    }
    else if (type == COMPOUND2) {
        //cerr << "cmp1 = " << cmp1 << " cmp2 = " << cmp2 << " type = "
        //     << (int)type << endl;
        cmp1 = new Id(compoundId1());
        cmp2 = new Id(compoundId2());
    }
    //else if (type == CUSTOM)
    //    data = (void *)controlFn(CF_COPY, data);
    else throw ML::Exception("unknown Id type");
}
    
void
Id::
serialize(ML::DB::Store_Writer & store) const
{
    store << (char)1 << (char)type;
    //cerr << "after header: " << store.offset() << endl;
    switch (type) {
    case NONE: break;
    case NULLID: break;
    case UUID:
    case GOOG128:
    case BIGDEC:
        store.save_binary(&val1, 8);
        store.save_binary(&val2, 8);
        break;
    case BASE64_96:
        store.save_binary(&val1, 8);
        store.save_binary(&val2, 4);
        break;
    case HEX128LC:
        store.save_binary(&val1, 8);
        store.save_binary(&val2, 8);
        break;
    case STR:
        store << string(str, str + len);
        break;
    case COMPOUND2:
        compoundId1().serialize(store);
        compoundId2().serialize(store);
        break;
    default:
        throw ML::Exception("unknown Id type");
    }

    //cerr << "after value: " << store.offset() << endl;
}

void
Id::
reconstitute(ML::DB::Store_Reader & store)
{
    Id r;

    char v, tp;
    store >> v;
    if (v < 0 || v > 1)
        throw ML::Exception("unknown Id version reconstituting");
    store >> tp;
    r.type = tp;

    // Fix up from earlier reconstitution version
    if (v == 0 && tp == 5)
        r.type = STR;

    if (v == 0) {
        // old domain field; no longer used
        int d;
        store >> d;
        //r.domain = d;
    }

    //cerr << "reading after header: " << store.offset() << endl;
    //cerr << "type = " << (int)r.type << endl;

    switch (r.type) {
    case NONE: break;
    case NULLID: break;
    case UUID:
    case GOOG128:
    case BIGDEC: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 8);
        break;
    }
    case BASE64_96: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 4);
        break;
    }
    case HEX128LC: {
        store.load_binary(&r.val1, 8);
        store.load_binary(&r.val2, 8);
        break;
    }
    case STR: {
        std::string s;
        store >> s;
        r.len = s.size();
        r.ownstr = true;
        char * s2 = new char[s.size()];
        r.str = s2;
        std::copy(s.begin(), s.end(), s2);
        break;
    }
    case COMPOUND2: {
        unique_ptr<Id> id1(new Id()), id2(new Id());
        store >> *id1 >> *id2;
        r.cmp1 = id1.release();
        r.cmp2 = id2.release();
        break;
    }
    default:
        throw ML::Exception("unknown Id type %d reconstituting",
                            tp);
    }

    //cerr << "reading after value: " << store.offset() << endl;
    //cerr << "reconstituted " << r << endl;

    *this = std::move(r);
}

Json::Value
Id::
toJson() const
{
    if (notNull())
        return toString();
    else return Json::Value();
}

Id
Id::
fromJson(const Json::Value & val)
{
    if (val.isInt())
        return Id(val.asInt());

    else if (val.isUInt())
        return Id(val.asUInt());

    else if (val.isNull())
        return Id();

    else return Id(val.asString());
}

} // namespace Datacratic

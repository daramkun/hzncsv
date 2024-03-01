#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <codecvt>
#include <locale>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

namespace hzn::csv {
#if __cplusplus >= 202000L
    using u8string = std::u8string;
    using u8string_view = std::u8string_view;
#else
    using u8string = std::string;
    using u8string_view = std::string_view;
#endif

    enum class parse_result {
        success,

        file_not_found,
        column_count_no_matched,
        argument_error,
        parse_error,

        no_csv,
    };

    enum class parse_encoding {
        utf8,
        utf16_big_endian,
        utf16_little_endian,
        utf32_big_endian,
        utf32_little_endian,

        detect_from_bom,
    };

    struct parse_options {
        char delimiter;
        bool trim;
        parse_encoding determine_bom;
        bool backslash_to_special;
        bool backslash_to_special_detect_error;

        explicit parse_options(char delimiter = ',',
                               bool trim = true,
                               parse_encoding determine_bom = parse_encoding::utf8,
                               bool backslash_to_special = true,
                               bool backslash_to_special_detect_error = true)
                : delimiter(delimiter)
                , trim(trim)
                , determine_bom(determine_bom)
                , backslash_to_special(backslash_to_special)
                , backslash_to_special_detect_error(backslash_to_special_detect_error) {

        }
    };

    constexpr uint8_t utf8_bom_bytes[] = { 0xEF, 0xBB, 0xBF };
    constexpr uint8_t utf16be_bom_bytes[] = { 0xFE, 0xFF };
    constexpr uint8_t utf16le_bom_bytes[] = { 0xFF, 0xFE };
    constexpr uint8_t utf32be_bom_bytes[] = { 0x00, 0x00, 0xFE, 0xFF };
    constexpr uint8_t utf32le_bom_bytes[] = { 0xFF, 0xFE, 0x00, 0x00 };

    static inline char16_t swap_endian(const char16_t us) {
        return (us >> 8) | (us << 8);
    }

    static inline char32_t swap_endian(const char32_t ui) {
        return (ui >> 24) | ((ui<<8) & 0x00FF0000) | ((ui>>8) & 0x0000FF00) | (ui << 24);
    }

    class stream_buffer {
    public:
#pragma warning (disable: 26495)
        stream_buffer(std::istream &stream, parse_encoding encoding)
                : _stream(stream)
                , _encoding(encoding)
                , _position(0)
                , _length(0) {
        }
#pragma warning (default: 26495)

    public:
        [[nodiscard]]
        inline uint32_t read() {
            if (_position >= _length) {
                _stream.read(reinterpret_cast<char*>(_buffer), 4096);
                _length = _stream.gcount();
                _position = 0;

                if (_length == 0) {
                    return 0;
                }
            }

            size_t tempPosition = _position;

            switch (_encoding) {
                case parse_encoding::utf8:
                    return _buffer[_position++];
                case parse_encoding::utf16_big_endian:
                    _position += 2;
                    return swap_endian(reinterpret_cast<char16_t*>(_buffer)[tempPosition >> 1]);
                case parse_encoding::utf16_little_endian:
                    _position += 2;
                    return reinterpret_cast<char16_t*>(_buffer)[tempPosition >> 1];
                case parse_encoding::utf32_big_endian:
                    _position += 4;
                    return swap_endian(reinterpret_cast<char32_t*>(_buffer)[tempPosition / 4]);
                case parse_encoding::utf32_little_endian:
                    _position += 4;
                    return reinterpret_cast<char32_t*>(_buffer)[tempPosition / 4];
                default:
                    return 0;
            }
        }

        [[nodiscard]]
        inline bool eof() const {
            return _stream.eof();
        }

    private:
        std::istream& _stream;
        parse_encoding _encoding;

        uint8_t _buffer[4096];
        size_t _position;
        size_t _length;
    };

    class string_builder {
    public:
        explicit string_builder(parse_encoding encoding)
                : _buffer(4096)
                , _encoding(encoding) {
            _buffer.clear();
        }

    public:
        inline string_builder& append(uint32_t ch) {
            switch (_encoding) {
                case parse_encoding::utf8:
                    _buffer.emplace_back(static_cast<char>(ch));
                    break;
                case parse_encoding::utf16_big_endian:
                case parse_encoding::utf16_little_endian:
                    _buffer.emplace_back(static_cast<char16_t>(ch));
                    break;
                case parse_encoding::utf32_big_endian:
                case parse_encoding::utf32_little_endian:
                    _buffer.emplace_back(ch);
                    break;
            }
            return *this;
        }

    public:
        inline void clear() {
            _buffer.clear();
        }

        inline bool empty() {
            return _buffer.empty();
        }

        inline u8string to_string(bool trim = false) {
            static std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> convert16;
            static std::wstring_convert<std::codecvt_utf8_utf16<char32_t>, char32_t> convert32;

            if (_buffer.empty()) {
                return {};
            }

            switch (_encoding) {
                case parse_encoding::utf8:
                {
                    auto start = _buffer.begin();
                    auto end = --_buffer.end();

                    while (trim && (start < end) && (*start == ' ' || *start == '\t' || *start == '\0')) {
                        ++start;
                    }
                    while (trim && (start < end) && (*end == ' ' || *end == '\t' || *end == '\0')) {
                        --end;
                    }

                    return {start, end + 1};
                }

                case parse_encoding::utf16_big_endian:
                case parse_encoding::utf16_little_endian:
                {
                    auto start = reinterpret_cast<const char16_t*>(_buffer.data());
                    auto end = start + _buffer.size() / sizeof(char16_t);

                    while (trim && (start < end) && (*start == ' ' || *start == '\t' || *start == '\0')) {
                        ++start;
                    }
                    while (trim && (start < end) && (*end == ' ' || *end == '\t' || *end == '\0')) {
                        --end;
                    }

                    const auto u16 = std::u16string(start, end + 1);
#if __cplusplus >= 202000L
                    const auto u8 = convert16.to_bytes(u16);
                    return {u8.begin(), u8.end()};
#else
                    return convert16.to_bytes(u16);
#endif
                }

                case parse_encoding::utf32_big_endian:
                case parse_encoding::utf32_little_endian:
                {
                    auto start = reinterpret_cast<const char32_t*>(_buffer.data());
                    auto end = start + _buffer.size() / sizeof(char32_t);

                    while (trim && (start < end) && (*start == ' ' || *start == '\t' || *start == '\0')) {
                        ++start;
                    }
                    while (trim && (start < end) && (*end == ' ' || *end == '\t' || *end == '\0')) {
                        --end;
                    }

                    const auto u32 = std::u32string(start, end + 1);
#if __cplusplus >= 202000L
                    const auto u8 = convert32.to_bytes(u32);
                    return {u8.begin(), u8.end()};
#else
                    return convert32.to_bytes(u32);
#endif
                }
            }

            return {};
        }

    private:
        std::vector<uint8_t> _buffer;
        parse_encoding _encoding;
    };

    class document {
    public:
        [[maybe_unused]]
        parse_result parse_from_file(const std::string_view& path, const parse_options& options) {
            std::ifstream stream;
            stream.open(std::string(path));
            if (!stream.is_open())
                return parse_result::file_not_found;
            return parse_from_stream(stream, options);
        }

        [[maybe_unused]]
        parse_result parse_from_text(const std::string_view& text, const parse_options& options) {
            std::istringstream stream((std::string(text)));
            return parse_from_stream(stream, options);
        }

        [[maybe_unused]]
        parse_result parse_from_stream(std::istream& stream, const parse_options& options) {
            parse_encoding parse_encoding = detect_encoding(stream, options.determine_bom);

            stream_buffer buffer(stream, parse_encoding);
            string_builder builder(parse_encoding);
            std::vector<u8string> row;
            bool is_in_quote = false;
            bool last_is_backslash = false;

            uint32_t read, lastRead = 0;
            while ((read = buffer.read()) != 0) {
                if (last_is_backslash) {
                    last_is_backslash = false;
                    if (read == '"') {
                        builder.append(double_quote_per_encoding(parse_encoding));
                        continue;
                    } else if (read == 'n') {
                        builder.append(nextline_per_encoding(parse_encoding));
                        continue;
                    } else if (read == '\\') {
                        builder.append(backslash_per_encoding(parse_encoding));
                        continue;
                    } else {
                        if (options.backslash_to_special_detect_error) {
                            _document.clear();
                            return parse_result::parse_error;
                        } else {
                            builder.append(backslash_per_encoding(parse_encoding));
                        }
                    }
                }

                if (read == options.delimiter && !is_in_quote) {
                    row.emplace_back(builder.to_string(options.trim));
                    builder.clear();
                } else if (read == '"' && !is_in_quote) {
                    is_in_quote = true;
                } else if (read == '"') {
                    if (lastRead == '"') {
                        builder.append(double_quote_per_encoding(parse_encoding));
                    } else {
                        is_in_quote = false;
                    }
                } else if (read == '\\' && options.backslash_to_special) {
                    last_is_backslash = true;
                } else if (read == '\n') {
                    if (is_in_quote) {
                        _document.clear();
                        return parse_result::parse_error;
                    }

                    if (row.empty() && builder.empty()) {
                        continue;
                    }

                    row.emplace_back(builder.to_string(options.trim));
                    builder.clear();

                    if (!_document.empty() && _document[0].size() != row.size()) {
                        return parse_result::column_count_no_matched;
                    }

                    _document.emplace_back(row);
                    row.clear();
                } else {
                    builder.append(read);
                }

                lastRead = read;
            }

            if (!row.empty()) {
                if (is_in_quote) {
                    _document.clear();
                    return parse_result::parse_error;
                }

                row.emplace_back(builder.to_string(options.trim));
                builder.clear();

                if (!_document.empty() && _document[0].size() != row.size()) {
                    return parse_result::column_count_no_matched;
                }

                _document.emplace_back(row);
            }

            return parse_result::success;
        }

    public:
        [[nodiscard]]
        size_t row_size() const { return _document.size(); }
        [[nodiscard]]
        size_t column_size() const {
            if (!_document.empty()) {
                return _document[0].size();
            }

            return 0;
        }

        [[nodiscard]]
        u8string_view column_raw(size_t row, size_t column) const {
            const auto& row_vector = _document[row];
            const auto& column_raw = row_vector[column];
            return column_raw;
        }

        [[nodiscard]]
        const std::vector<u8string>& row(size_t row) const {
            return _document[row];
        }

        std::vector<std::vector<u8string>>::iterator begin() {
            return _document.begin();
        }

        std::vector<std::vector<u8string>>::iterator end() {
            return _document.end();
        }

    private:
        std::vector<std::vector<u8string>> _document;

    private:
        static inline bool is_equal_array(const uint8_t* array1, const uint8_t* array2, size_t size) {
            switch (size) {
                case 2:
                    return *reinterpret_cast<const uint16_t*>(array1) == *reinterpret_cast<const uint16_t*>(array2);
                case 3:
                    return *reinterpret_cast<const uint16_t*>(array1) == *reinterpret_cast<const uint16_t*>(array2) &&
                           array1[2] == array2[2];
                case 4:
                    return *reinterpret_cast<const uint32_t*>(array1) == *reinterpret_cast<const uint32_t*>(array2);

                default:
                    return memcmp(array1, array2, size) == 0;
            }
        }

        static inline parse_encoding detect_encoding(std::istream& stream, parse_encoding encoding) {
            if (encoding == parse_encoding::detect_from_bom) {
                uint8_t buffer[4];
                stream.read(reinterpret_cast<char*>(buffer), 4);
                const auto read_bom = stream.gcount();
                if (read_bom == 0) {
                    return parse_encoding::detect_from_bom;
                }

                if (is_equal_array(buffer, utf8_bom_bytes, sizeof(utf8_bom_bytes))) {
                    stream.seekg(3);
                    return parse_encoding::utf8;
                } else if (is_equal_array(buffer, utf16be_bom_bytes, sizeof(utf16be_bom_bytes))) {
                    stream.seekg(2);
                    return parse_encoding::utf16_big_endian;
                } else if (is_equal_array(buffer, utf16le_bom_bytes, sizeof(utf16le_bom_bytes))) {
                    stream.seekg(2);
                    return parse_encoding::utf16_little_endian;
                } else if (is_equal_array(buffer, utf32be_bom_bytes, sizeof(utf32be_bom_bytes))) {
                    stream.seekg(4);
                    return parse_encoding::utf32_big_endian;
                } else if (is_equal_array(buffer, utf32le_bom_bytes, sizeof(utf32le_bom_bytes))) {
                    stream.seekg(4);
                    return parse_encoding::utf32_little_endian;
                }
            }

            return encoding;
        }

        static inline uint32_t double_quote_per_encoding(parse_encoding encoding) {
            static uint32_t cached[] = {
                    '"',
                    u'"',
                    u'"',
                    U'"',
                    U'"'
            };
            return cached[(int)encoding];
        }

        static inline uint32_t backslash_per_encoding(parse_encoding encoding) {
            static uint32_t cached[] = {
                    '\\',
                    u'\\',
                    u'\\',
                    U'\\',
                    U'\\'
            };
            return cached[(int)encoding];
        }

        static inline uint32_t nextline_per_encoding(parse_encoding encoding) {
            static uint32_t cached[] = {
                    '\n',
                    u'\n',
                    u'\n',
                    U'\n',
                    U'\n'
            };
            return cached[(int)encoding];
        }
    };
}
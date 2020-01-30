package com.booking.replication.augmenter.model.format;


import java.util.HashMap;
import java.util.Map;

/*
 * MySQL Collations and their IDs:
 *
 * Retrieved from: https://dev.mysql.com/doc/internals/en/character-set.html
 *
 *  CollationName =>  CollationId
 *  big5_chinese_ci	    1
 *  dec8_swedish_ci  	3
 *  cp850_general_ci	4
 *  hp8_english_ci	    6
 *  koi8r_general_ci	7
 *  latin1_swedish_ci	8
 *  latin2_general_ci	9
 *  swe7_swedish_ci	    10
 *  ascii_general_ci	11
 *  ujis_japanese_ci	12
 *  sjis_japanese_ci	13
 *  hebrew_general_ci	16
 *  tis620_thai_ci	    18
 *  euckr_korean_ci	    19
 *  koi8u_general_ci	22
 *  gb2312_chinese_ci	24
 *  greek_general_ci	25
 *  cp1250_general_ci	26
 *  gbk_chinese_ci	    28
 *  latin5_turkish_ci	30
 *  armscii8_general_ci	32
 *  utf8_general_ci	    33
 *  ucs2_general_ci	    35
 *  cp866_general_ci	36
 *  keybcs2_general_ci	37
 *  macce_general_ci	38
 *  macroman_general_ci	39
 *  cp852_general_ci	40
 *  latin7_general_ci	41
 *  cp1251_general_ci	51
 *  utf16_general_ci	54
 *  utf16le_general_ci	56
 *  cp1256_general_ci	57
 *  cp1257_general_ci	59
 *  utf32_general_ci	60
 *  binary	            63
 *  geostd8_general_ci	92
 *  cp932_japanese_ci	95
 *  eucjpms_japanese_ci	97
 *  gb18030_chinese_ci	248
 *  utf8mb4_0900_ai_ci	255
 */
public enum MySQLCollation {

    big5_chinese_ci(1),
    dec8_swedish_ci(3),
    cp850_general_ci(4),
    hp8_english_ci(6),
    koi8r_general_ci(7),
    latin1_swedish_ci(8),
    latin2_general_ci(9),
    swe7_swedish_ci(10),
    ascii_general_ci(11),
    ujis_japanese_ci(12),
    sjis_japanese_ci(13),
    hebrew_general_ci(16),
    tis620_thai_ci(18),
    euckr_korean_ci(19),
    koi8u_general_ci(22),
    gb2312_chinese_ci(24),
    greek_general_ci(25),
    cp1250_general_ci(26),
    gbk_chinese_ci(28),
    latin5_turkish_ci(30),
    armscii8_general_ci(32),
    utf8_general_ci(33),
    ucs2_general_ci(35),
    cp866_general_ci(36),
    keybcs2_general_ci(37),
    macce_general_ci(38),
    macroman_general_ci(39),
    cp852_general_ci(40),
    latin7_general_ci(41),
    cp1251_general_ci(51),
    utf16_general_ci(54),
    utf16le_general_ci(56),
    cp1256_general_ci(57),
    cp1257_general_ci(59),
    utf32_general_ci(60),
    binary(63),
    geostd8_general_ci(92),
    cp932_japanese_ci(95),
    eucjpms_japanese_ci(97),
    gb18030_chinese_ci(248),
    utf8mb4_0900_ai_ci(255);

    private int code;

    MySQLCollation(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    private static final Map<Integer, MySQLCollation> INDEX_BY_CODE;

    static {
        INDEX_BY_CODE = new HashMap<Integer, MySQLCollation>();
        for (MySQLCollation mySQLCollation : values()) {
            INDEX_BY_CODE.put(mySQLCollation.code, mySQLCollation);
        }
    }

    public static MySQLCollation byCode(int code) {
        return INDEX_BY_CODE.get(code);
    }

}

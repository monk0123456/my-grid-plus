package org.gridgain.temp;

import org.junit.Test;
import org.tools.MyConvertUtil;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyDateUitl {

    public static String getFormTime(final String line)
    {
        Pattern cls = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}$|^\\d{4}-\\d{2}-\\d{1}$|\\d{4}-\\d{1}-\\d{2}$|\\d{4}-\\d{1}-\\d{1}$", Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        Matcher matcher = cls.matcher(line);
        if (matcher.find())
        {
            return "yyyy-MM-dd";
        }
        return "";
    }

    public static String getFormTime(final String line, final String patternLine, final String value)
    {
        Pattern cls = Pattern.compile(patternLine, Pattern.CASE_INSENSITIVE|Pattern.MULTILINE);
        Matcher matcher = cls.matcher(line);
        if (matcher.find())
        {
            return value;
        }
        return null;
    }

    public static String myFormTime(final String line)
    {
        Hashtable<String, String> ht = new Hashtable<>();
        ht.put("^\\d{4}-\\d{2}-\\d{2}$|^\\d{4}-\\d{2}-\\d{1}$|^\\d{4}-\\d{1}-\\d{2}$|^\\d{4}-\\d{1}-\\d{1}$", "yyyy-MM-dd");
        ht.put("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}-\\d{2}-\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}-\\d{1}-\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}-\\d{1}-\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        ht.put("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}-\\d{2}-\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}-\\d{1}-\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}-\\d{1}-\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$", "yyyy-MM-dd HH:mm:ss.SSS");
        ht.put("^\\d{4}/\\d{2}/\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}/\\d{2}/\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}/\\d{1}/\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$|^\\d{4}/\\d{1}/\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        ht.put("^\\d{4}/\\d{2}/\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}/\\d{2}/\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}/\\d{1}/\\d{2}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$|^\\d{4}/\\d{1}/\\d{1}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$", "yyyy/MM/dd HH:mm:ss.SSS");
        ht.put("^\\d{4}/\\d{2}/\\d{2}$|^\\d{4}/\\d{2}/\\d{1}$|^\\d{4}/\\d{1}/\\d{2}$|^\\d{4}/\\d{1}/\\d{1}$", "yyyy/MM/dd");
        ht.put("^\\d{8}\\s+\\d{2}\\:\\d{2}\\:\\d{2}$", "yyyyMMdd HH:mm:ss");
        ht.put("^\\d{8}\\s+\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}$", "yyyyMMdd HH:mm:ss.SSS");
        ht.put("^\\d{8}$", "yyyyMMdd");
        ht.put("^\\d{17}$", "yyyyMMddHHmmssSSS");
        ht.put("^\\d{14}$", "yyyyMMddHHmmss");

        for (String p : ht.keySet())
        {
            String tp = getFormTime(line, p, ht.get(p));
            if (tp != null)
                return tp;
        }
        return null;
    }

    public static Timestamp ConvertToTimestamp(final Object t) {
        //String format = MyConvertUtil.myFormTime(t.toString());
        String format = myFormTime(t.toString());
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        try {
            Date date = simpleDateFormat.parse(t.toString());
            Timestamp timestamp = new Timestamp(date.getTime());
            return timestamp;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void my_case() throws Exception {
        Object line = "1992-05-01";
        System.out.println(ConvertToTimestamp("1996-08-16 00:10:02"));
        //System.out.println(MyConvertUtil.ConvertToTimestamp(line));
    }

    @Test
    public void my_case_1()
    {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try {
            Date date = simpleDateFormat.parse("1996-08-16 00:10:02.032");
            Timestamp timestamp = new Timestamp(date.getTime());
            System.out.println(timestamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}











































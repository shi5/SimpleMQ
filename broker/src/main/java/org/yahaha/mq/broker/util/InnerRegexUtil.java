package org.yahaha.mq.broker.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ReUtil;

import java.util.List;
import java.util.regex.Pattern;

public class InnerRegexUtil {
    public static boolean hasMatch(List<String> tagNameList,
                                   String tagRegex) {
        if(CollectionUtil.isEmpty(tagNameList)) {
            return false;
        }

        Pattern pattern = Pattern.compile(tagRegex);

        for(String tagName : tagNameList) {
            if(ReUtil.isMatch(pattern, tagName)) {
                return true;
            }
        }

        return false;
    }
}

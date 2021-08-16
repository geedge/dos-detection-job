package com.zdjizhi.utils;

import java.util.Collection;
import java.util.HashSet;

/**
 * @author wlh
 * 扩展集合处理工具
 */
public class CollectionUtils {

    public static<T> Collection<T> takeUniqueLimit(Collection<T> collection, int limit){
        int i =0;
        Collection<T> newSet = new HashSet<>();
        for (T t:collection){
            if (i < limit){
                newSet.add(t);
                i += 1;
            }
        }
        return newSet;
    }
}

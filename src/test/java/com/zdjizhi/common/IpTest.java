package com.zdjizhi.common;

import inet.ipaddr.Address;
import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.format.util.AddressTrieMap;
import inet.ipaddr.format.util.AssociativeAddressTrie;
import inet.ipaddr.ipv4.IPv4Address;
import inet.ipaddr.ipv4.IPv4AddressAssociativeTrie;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IpTest {
    public static void main(String[] args) throws Exception {
        IPv4AddressAssociativeTrie<Integer> trie = new IPv4AddressAssociativeTrie<>();

        IPAddress str1 = new IPAddressString("1.2.3.4").getAddress();
        IPAddress str2 = new IPAddressString("10.0.0.0/15").getAddress();
        IPAddress str3 = new IPAddressString("25.4.2.0/23").getAddress();
        IPAddress str4 = new IPAddressString("192.168.8.0/21").getAddress();
        IPAddress str5 = new IPAddressString("240.0.0.0/4").getAddress();
        IPAddress str6 = new IPAddressString("fc00::0/64").getAddress();
        IPAddress str7 = new IPAddressString("fc00::10:1").getAddress();

        TreeRangeMap<IPAddress, Object> rangeMap = TreeRangeMap.create();
        rangeMap.put(Range.closed(str1.getLower(),str1.getUpper()),1);
        rangeMap.put(Range.closed(str2.getLower(),str2.getUpper()),2);
        rangeMap.put(Range.closed(str3.getLower(),str3.getUpper()),3);
        rangeMap.put(Range.closed(str4.getLower(),str4.getUpper()),4);
        rangeMap.put(Range.closed(str5.getLower(),str5.getUpper()),5);
        rangeMap.put(Range.closed(str6.getLower(),str6.getUpper()),6);
        rangeMap.put(Range.closed(str7.getLower(),str7.getUpper()),7);

        IPAddress pv4 = new IPAddressString("255.255.14.255").getAddress();
        IPAddress pv42 = new IPAddressString("1.2.3.4").getAddress();
        IPAddress pv43 = new IPAddressString("fc00::").getAddress();
        IPAddress pv44 = new IPAddressString("fc00::10:1").getAddress();

        IPAddress pv45 = new IPAddressString("192.168.42.1/32").getAddress();
        IPAddress pv46 = new IPAddressString("192.168.42.1/32").getAddress();
        IPAddress pv47 = new IPAddressString("12.56.4.0").getAddress();
        System.out.println(pv45.isMultiple());
        System.out.println(pv46.isMultiple());
        System.out.println(pv46.isPrefixed());
        System.out.println(pv47.isPrefixed());
        System.out.println(pv45+"---"+pv45.toMaxHost().withoutPrefixLength()+"---"+pv45.adjustPrefixLength(pv45.getBitCount()));

        System.out.println(pv45.adjustPrefixLength(pv45.getBitCount())+"---"+pv45.toMaxHost().withoutPrefixLength());

        /*
        System.out.println(str5.getUpper()+"---"+str5.getLower());

        System.out.println(rangeMap.span().contains(pv4));

        System.out.println(rangeMap.get(pv4));
        System.out.println(rangeMap.get(pv42));
        System.out.println(rangeMap.get(pv43));
        System.out.println(rangeMap.get(pv44));
        */

        /*
        System.out.println(str5.toSequentialRange());
//        System.out.println(str2.contains(new IPAddressString("10.0.0.2")));
//        System.out.println(str5.toAddress().toIPv4().toSequentialRange());


        trie.put(str1,1);
        trie.put(str2,2);
        trie.put(str3,3);
        trie.put(str4,4);
        trie.put(str5,5);

        AddressTrieMap<IPv4Address, Integer> trieMap = new AddressTrieMap<>(trie);



        trieMap.forEach((k,v) -> {
            System.out.println(k.toString() + "--" + v);
        });

        System.out.println("-----------------");

        trie.forEach((k) -> System.out.println(k.toString()));

        System.out.println(str5.contains(pv4));
        System.out.println(trie.contains(pv4));
        System.out.println(trieMap.get(pv4));
        System.out.println(trieMap.containsKey(pv4));
//        System.out.println(trieMap.getRange());
//        IPAddress str3 = new IPAddressString("fc00::10:1").getAddress();
//        IPAddress str4 = new IPAddressString("fc00::10:2/64").getAddress();

//        System.out.println(Arrays.toString(str1.mergeToPrefixBlocks(str2,str3,str4)));

    */
    }
}

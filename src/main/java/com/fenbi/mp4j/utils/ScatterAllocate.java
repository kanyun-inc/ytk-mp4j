/**
*
* Copyright (c) 2017 ytk-mp4j https://github.com/yuantiku
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

package com.fenbi.mp4j.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xialong
 */
public class ScatterAllocate {

    public static void main(String []args) {
        for (int p = 13; p < 14; p++) {
            for (int root = 0; root < p; root ++) {
                System.out.println("root=" + root + "-->" + allocate(p, root));
                System.out.println("recvnum>>>" + recvNum(allocate(p, root)));
                if (p % 100 == 0) {
                    System.out.println("p=" + p);
                }
                Map<Integer, Integer> recvNumMap = recvNum(allocate(p, root));
                for (int i = 0; i < p; i++) {
                    int num = recvNumMap.getOrDefault(i, 0);
                    if (num >= 2) {
                        System.out.println("error");
                        System.exit(1);
                    }

                }
            }
        }

    }

    public static Map<Integer, List<List<Integer>>> allocate(int p, int root) {
        List<Integer> splitList = new ArrayList<>();
        int mid = p / 2;

        if (root != 0) {
            splitList.add(root);
            splitList.add(0);
            splitList.add(0);
            splitList.add(mid - 1);
        }

        if (root != mid) {
            splitList.add(root);
            splitList.add(mid);
            splitList.add(mid);
            splitList.add(p - 1);
        }

        split(0, mid - 1, splitList);
        split(mid, p - 1, splitList);
        Map<Integer, List<List<Integer>>> allocateMap = new HashMap<>(p);
        for (int i = 0; i < splitList.size(); i += 4) {
            List<Integer> list = new ArrayList<>(4);
            list.add(splitList.get(i));
            list.add(splitList.get(i + 1));
            list.add(splitList.get(i + 2));
            list.add(splitList.get(i + 3));
            int src = splitList.get(i);
            List<List<Integer>> listList = allocateMap.get(src);
            if (listList == null) {
                listList = new ArrayList<>();
            }
            listList.add(list);
            allocateMap.put(src, listList);
        }

        return allocateMap;
    }

    public static void split(int start, int end, List<Integer> retList) {
        if (start == end) {
            return;
        }

        int size = end - start + 1;
        int half = size / 2;
        retList.add(start);
        retList.add(start + half);
        retList.add(start + half);
        retList.add(end);

        split(start, start + half - 1, retList);
        split(start + half, end, retList);
    }


    public static Map<Integer, Integer> recvNum(Map<Integer, List<List<Integer>>> allocateMap) {
        Map<Integer, Integer> recvNumMap = new HashMap<>();
        for (Map.Entry<Integer, List<List<Integer>>> entry : allocateMap.entrySet()) {
            List<List<Integer>> info = entry.getValue();
            for (List<Integer> list : info) {
                int recvrank = list.get(1);
                Integer cnt = recvNumMap.get(recvrank);
                if (cnt == null) {
                    cnt = new Integer(1);
                    recvNumMap.put(recvrank, cnt);
                } else {
                    recvNumMap.put(recvrank, cnt + 1);
                }

            }
        }
        return recvNumMap;
    }
}

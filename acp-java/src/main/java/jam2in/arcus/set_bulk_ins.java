/* -*- Mode: Java; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
/*
 * acp-java : Arcus Java Client Performance benchmark program
 * Copyright 2013-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jam2in.arcus;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

public class set_bulk_ins implements client_profile {

  public boolean do_test(client cli) {
    try {
      if (!do_set_test(cli))
        return false;
    } catch (Exception e) {
      System.out.printf("client_profile exception. id=%d exception=%s\n",
                        cli.id, e.toString());
      if (cli.conf.print_stack_trace)
        e.printStackTrace();
    }
    return true;
  }

  public boolean do_set_test(client cli) throws Exception {
    int loop_cnt = 10;

    // Prepare Key list
    List<String> key_list = new LinkedList<String>();

    for (int i = 0; i < loop_cnt; i++) {
      String key = cli.ks.get_key();
      key_list.add(key);
    }
    String[] temp = key_list.get(0).split("-");
    long base = Long.parseLong(temp[1]);
    base = base * 64*1024;

    // Create a set item
    ElementValueType vtype = ElementValueType.BYTEARRAY;
    CollectionAttributes attr =
      new CollectionAttributes(cli.conf.client_exptime,
                               CollectionAttributes.DEFAULT_MAXCOUNT,
                               CollectionOverflowAction.error);

    for (int i = 0; i < loop_cnt; i++) {
      if (!cli.before_request())
        return false;
      CollectionFuture<Boolean> fb = 
        cli.next_ac.asyncSopCreate(key_list.get(i), vtype, attr);

      boolean ok = fb.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);
      if (!ok) {
        System.out.printf("sop create failed. id=%d key=%s : %s\n", cli.id,
                          key_list.get(i), fb.getOperationStatus().getResponse());
      }
      if (!cli.after_request(ok))
        return false;
      if (!ok)
        return true;
    }

    // repeat 1000
    for (long skey = base; skey < base + 1000; skey++) {
      if (!cli.before_request())
        return false;
      byte[] val = cli.vset.get_value();
      assert(val.length <= 4096);

      long n = skey;
      int i = 0;
      while (n != 0 && i < val.length) {
        val[i] = (byte)(n % 10);
        n = n / 10;
        i++;
      }

      Future<Map<String, CollectionOperationStatus>> fbs =
        cli.next_ac.asyncSopInsertBulk(key_list, val, new CollectionAttributes());
      Map<String, CollectionOperationStatus> result = fbs.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);

      if (!cli.after_request(true))
        return false;
   }

   return true;
  }
}


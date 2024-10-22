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

public class simple_async_decr implements client_profile {

  public boolean do_test(client cli) {
    try {
      if (!do_simple_test(cli))
        return false;
    } catch (Exception e) {
      System.out.printf("client_profile exception. id=%d exception=%s\n",
                        cli.id, e.toString());
      if (cli.conf.print_stack_trace)
        e.printStackTrace();
    }
    return true;
  }

  public boolean do_simple_test(client cli) throws Exception {
    int by = 1;

    if (!cli.before_request())
      return false;

    String key = cli.ks.get_key();
    String val = "10000";

    // SET
    Future<Boolean> fb =
      cli.next_ac.set(key, cli.conf.client_exptime, val);
    boolean ok = fb.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);
    if (!cli.after_request(ok))
      return false;
    if (!ok)
      return true;

    // Decr 100 times.
    for (int i = 0; i < 100; i++) {
      if (!cli.before_request())
        return false;

      Future<Long> f = cli.next_ac.asyncDecr(key, by);
      Long result = f.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);
      if (result == null) {
        System.out.printf("key-value Decr failed. id=%d\n", cli.id);
      }
      if (!cli.after_request(true))
        return false;
      if (result == null)
        return true;
    }

    return true;
  }
}

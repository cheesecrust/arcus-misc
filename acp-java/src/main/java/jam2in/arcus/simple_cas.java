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

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;

public class simple_cas implements client_profile {
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
    // Pick a key
    String key = cli.ks.get_key();

    // Insert on item
    if (!cli.before_request())
      return false;
    byte[] val = cli.vset.get_value();
    Future<Boolean> fb =
      cli.next_ac.set(key, cli.conf.client_exptime, val, raw_transcoder.raw_tc);
    boolean ok = fb.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);
    if (!ok) {
      System.out.printf("set failed. id=%d key=%s\n", cli.id, key);
    }
    if (!cli.after_request(ok))
      return false;
    if (!ok)
      return true;

    // Gets
    if (!cli.before_request())
      return false;
    Future<CASValue<byte[]>> fcv =
      cli.next_ac.asyncGets(key, raw_transcoder.raw_tc);
    CASValue<byte[]> casv = fcv.get(cli.conf.client_timeout, TimeUnit.MILLISECONDS);
    ok = (casv != null);
    if (!ok) {
      System.out.printf("gets failed. id=%d key=%s\n", cli.id, key);
    }
    if (!cli.after_request(ok))
      return false;
    if (!ok)
      return true;
    byte[] get_val = casv.getValue();
    long cas_num = casv.getCas();
    if (!Arrays.equals(val, get_val)) {
      System.out.printf("gets value does not match the original value." +
                        " id=%d key=%s\n", cli.id, key);
      return true;
    }

    // CAS
    if (!cli.before_request())
      return false;
    val = cli.vset.get_value();
    CASResponse fcr =
      cli.next_ac.cas(key, cas_num+1, val, raw_transcoder.raw_tc);
    if (fcr == null) {
      System.out.println("CAS returns an unexpected OK response." +
                         " id=" + cli.id + " key=" + key);
    }
    if (!cli.after_request(ok))
      return false;
    if (fcr == null)
      return true;

    // Cas
    if (!cli.before_request())
      return false;
    fcr = cli.next_ac.cas(key, cas_num, val, raw_transcoder.raw_tc);
    if (fcr == null) {
      System.out.println("CAS returns an unexpected non-OK response." +
                         " id=" + cli.id + " key=" + key);
    }
    if (!cli.after_request(ok))
      return false;

    return true;
  }
}

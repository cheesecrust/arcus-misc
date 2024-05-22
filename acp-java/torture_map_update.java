import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.*;

import net.spy.memcached.internal.CollectionFuture;

public class torture_map_update implements client_profile {

  private final int sizeOfMaps;
  private final int sizeOfUpdateElements;
  private final List<String> mkeys;

  public torture_map_update(int sizeOfMaps, int sizeOfUpdateElements) {
    this.sizeOfMaps = sizeOfMaps;
    this.sizeOfUpdateElements = sizeOfUpdateElements;
    this.mkeys = IntStream.range(0, sizeOfUpdateElements).boxed().map(integer -> "element-" + integer).collect(Collectors.toList());
  }

  @Override
  public boolean do_test(client cli) {
    if (!cli.before_request())
      return false;
    Boolean succeed = true;
    try {
      String key = "testMap:key1";
      String randomValue = System.currentTimeMillis() + " random value";
      List<CollectionFuture<Boolean>> futures = new ArrayList<CollectionFuture<Boolean>>();
      for(String mkey : mkeys) {
        CollectionFuture<Boolean> future = cli.next_ac.asyncMopUpdate(key, mkey, randomValue);
        futures.add(future);
      }

      for(CollectionFuture<Boolean> future : futures) {
        Boolean ok = future.get(700, TimeUnit.MILLISECONDS);
        if (!ok) {
          succeed = false;
          System.out.printf("Failed to update element. id=%d key=%s\n", cli.id, key);
        }
      }
    } catch (Exception e) {
      System.out.printf("client_profile exception. id=%d exception=%s\n", cli.id, e);
      if (cli.conf.print_stack_trace) {
        e.printStackTrace();
      }
    }
    if (!cli.after_request(succeed))
      return false;

    return true;
  }
}

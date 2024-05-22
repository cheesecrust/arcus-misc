import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.*;

public class torture_map_pipe_update implements client_profile {

  private final int sizeOfMaps;
  private final int sizeOfUpdateElements;
  private final List<String> mkeys;

  public torture_map_pipe_update(int sizeOfMaps, int sizeOfUpdateElements) {
    this.sizeOfMaps = sizeOfMaps;
    this.sizeOfUpdateElements = sizeOfUpdateElements;
    this.mkeys = IntStream.range(0, sizeOfUpdateElements).boxed().map(integer -> "element-" + integer).collect(Collectors.toList());
  }

  private Map<String, Object> getElements() {
    Map<String, Object> map = new HashMap<>();
    long value = System.currentTimeMillis();
    for (String mkey : mkeys) {
      map.put(mkey, value);
    }
    return map;
  }

  @Override
  public boolean do_test(client cli) {
    if (!cli.before_request())
      return false;
    Boolean succeed = true;
    try {
      String key = "testMap:key1";
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future = cli.next_ac.asyncMopPipedUpdateBulk(key, getElements());
      Map<Integer, CollectionOperationStatus> map = future.get(700, TimeUnit.MILLISECONDS);
      if (!map.isEmpty()) {
        succeed = false;
        System.out.printf("Failed to update element. id=%d failed=%s\n", cli.id, map);
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

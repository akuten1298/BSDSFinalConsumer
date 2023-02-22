import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author aakash
 */
public class DataStore {

  private String userId;

  Map<String, List<String>> swipeStore;

  public DataStore(String userId) {
    this.userId = userId;
    swipeStore = new HashMap<>();
  }

  public void updateStore(String direction, String swipeeId) {
    List<String> swipeListForUserDirection = swipeStore.get(direction);
    if(swipeListForUserDirection == null)
      swipeListForUserDirection = new ArrayList<>();
    swipeListForUserDirection.add(swipeeId);
    swipeStore.put(direction, swipeListForUserDirection);
  }

  public void getListContents() {
    for (String key : swipeStore.keySet()) {
      List<String> value = swipeStore.get(key);
      System.out.println(key);
      for (String str : value) {
        System.out.println(str);
      }
    }
  }

  public String getUserId() {
    return userId;
  }

  public Map<String, List<String>> getSwipeStore() {
    return swipeStore;
  }
}

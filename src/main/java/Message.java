import java.util.ArrayList;
import java.util.List;

/**
 * @author aakash
 */
public class Message {

  private String swiperId;
  private String swipeeId;
  private String comment;
  private String swipeDirection;

  public Message(String[] messageContentsFromQueue) {
    List<String> filteredContent = new ArrayList<>();
    for(String content : messageContentsFromQueue) {
      int equalsIndex = content.indexOf("=");
      filteredContent.add(content.substring(equalsIndex + 1));
    }

    swiperId = filteredContent.get(0);
    swipeeId = filteredContent.get(1);
    comment = filteredContent.get(2);
    swipeDirection = filteredContent.get(3);
  }

  @Override
  public String toString() {
    return "Message{" +
        "swiperId='" + swiperId + '\'' +
        ", swipeeId='" + swipeeId + '\'' +
        ", comment='" + comment + '\'' +
        ", swipeDirection='" + swipeDirection + '\'' +
        '}';
  }

  public String getSwiperId() {
    return swiperId;
  }

  public String getSwipeeId() {
    return swipeeId;
  }

  public String getComment() {
    return comment;
  }

  public String getSwipeDirection() {
    return swipeDirection;
  }
}

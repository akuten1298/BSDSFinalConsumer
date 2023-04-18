import java.util.ArrayList;
import java.util.List;

/**
 * @author aakash
 */
public class Message {

  private String swiper;
  private String swipee;
  private String comment;
  private String swipeDirection;

  public Message(String[] messageContentsFromQueue) {
    List<String> filteredContent = new ArrayList<>();
    for(String content : messageContentsFromQueue) {
      int equalsIndex = content.indexOf("=");
      filteredContent.add(content.substring(equalsIndex + 1));
    }

    swiper = filteredContent.get(0);
    swipee = filteredContent.get(1);
    comment = filteredContent.get(2);
    swipeDirection = filteredContent.get(3);
  }

  @Override
  public String toString() {
    return "Message{" +
        "swiper='" + swiper + '\'' +
        ", swipee='" + swipee + '\'' +
        ", comment='" + comment + '\'' +
        ", swipeDirection='" + swipeDirection + '\'' +
        '}';
  }

  public String getSwiper() {
    return swiper;
  }

  public String getSwipee() {
    return swipee;
  }

  public String getComment() {
    return comment;
  }

  public String getSwipeDirection() {
    return swipeDirection;
  }
}

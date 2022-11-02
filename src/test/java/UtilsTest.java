import dev.donhk.pojos.CarInformation;
import dev.donhk.pojos.Comment;
import dev.donhk.pojos.UserTxn;
import dev.donhk.utilities.Utils;
import org.junit.jupiter.api.Test;

import java.util.List;

public class UtilsTest {
    @Test
    public void test1() {
        final List<UserTxn> list = Utils.getUserTxnList();
        list.stream()
                .limit(5)
                .forEach(System.out::println);
    }

    @Test
    public void test2() {
        final List<Comment> list = Utils.getCommentList();
        list.stream()
                .limit(5)
                .forEach(System.out::println);
    }

    @Test
    public void test3() {
        final List<CarInformation> list = Utils.getCarInfoList();
        list.stream()
                .limit(5)
                .forEach(System.out::println);
    }
}

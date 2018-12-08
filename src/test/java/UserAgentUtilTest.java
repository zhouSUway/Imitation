import com.qianfeng.util.UserAgentUtil;

public class UserAgentUtilTest {

    public static void main(String[] args) {

        System.out.println(new UserAgentUtil().parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0"));
        System.out.println(new UserAgentUtil().parserUserAgent("Mozilla/5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F58.0.3029.110%20Safari%2F537.36%20SE%202.X%20MetaSr%201.0"));


    }
}

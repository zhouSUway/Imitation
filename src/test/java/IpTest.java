import com.qianfeng.util.IpUtil;

public class IpTest {
    public static void main(String[] args) {
        IpUtil.RegionInfo ip = IpUtil.parserIp("172.160.19.111");
        System.out.println(ip);
    }
}

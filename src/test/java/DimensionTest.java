import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatformDimension;
import com.qianfeng.analystic.mr.service.IDimensionConvert;
import com.qianfeng.analystic.mr.service.impl.IDimensionConvertImpl;
import com.qianfeng.common.DateEnum;
import org.apache.xerces.dom.PSVIAttrNSImpl;

import java.io.IOException;
import java.sql.SQLException;

public class DimensionTest {
    public static void main(String[] args) {


        DateDimension dd = DateDimension.buildDate((long) 12213323.4444,DateEnum.DAY);
        BrowserDimension br = new BrowserDimension("IE","11");

        PlatformDimension pl = new PlatformDimension("www");

        KpiDimension kp = new KpiDimension("bigdata");


        IDimensionConvert id = new IDimensionConvertImpl();


        try {
            System.out.println(id.getDimensionInterfaceById(kp));
            System.out.println(id.getDimensionInterfaceById(br));
            System.out.println(id.getDimensionInterfaceById(dd));
            System.out.println(id.getDimensionInterfaceById(pl));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}

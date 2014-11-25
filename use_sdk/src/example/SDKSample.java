package example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

public class SDKSample {

    private static String accessId;
    private static String accessKey;
    private static String endpoint;
    private static String project;

    public static void main(String[] args) throws OdpsException {
        // get conf
        getConf(args);

        // init
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);

        // run sql
        String tableName = "tmp_dual";
        String sql = "insert overwrite table " + tableName
                + " select id from dual;";
        System.out.println("[SQL] " + sql);
        Instance instance = SQLTask.run(odps, sql);
        if (!instance.isSync()) {
            instance.waitForSuccess();
        }
        System.out.println("[StartTime] " + instance.getStartTime().toString()
                + ", [EndTime] " + instance.getEndTime().toString());

        // get status
        if (instance.isSuccessful()) {
            System.out.println("[Status] success");
        } else {
            System.out.println("[Status] fail");
            System.exit(2);
        }

        // get result table size
        Table table = odps.tables().get(tableName);
        long size = table.getSize();
        System.out.println("table:" + tableName + ", size:" + size);
    }

    private static void getConf(String args[]) {
        if (args.length != 1) {
            System.err.println("Usage: SDKSample <odps.conf>");
            System.exit(2);
        }
        try {
            InputStream is = new FileInputStream(args[0]);
            Properties props = new Properties();
            props.load(is);
            accessId = props.getProperty("access.id");
            accessKey = props.getProperty("access.key");
            project = props.getProperty("default.project");
            endpoint = props.getProperty("endpoint");
        } catch (IOException e) {
            throw new IllegalArgumentException("Error reading ODPS config file '"
                    + args[0] + "'.");
        }
    }

}


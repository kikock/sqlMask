package yhh.com.mask.policy.file;


import yhh.com.mask.policy.AbstractPolicyStorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesFilePolicyStorage extends AbstractPolicyStorage {

    @Override
    public Map<String, String> loadPolicies(String source) {
        Path path = Paths.get("core-policy/src/main/resources/mysql-mask-policies.properties");
        File file = new File(path.toAbsolutePath().toString());
        Properties properties = new Properties();
        Map<String, String> map = new HashMap<>();
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            properties.load(in);
            in.close();
            map = new HashMap<String, String>((Map) properties);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return map;

    }
}
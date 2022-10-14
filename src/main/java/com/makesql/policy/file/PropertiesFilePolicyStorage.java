package com.makesql.policy.file;




import com.makesql.policy.AbstractPolicyStorage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesFilePolicyStorage extends AbstractPolicyStorage {

    @Override
    public Map<String, String> loadPolicies(String source) {
        String path =
                this.getClass().getClassLoader().getResource("mysql-mask-policies.properties").getPath();
        Properties properties = new Properties();
        Map<String, String> map = new HashMap<>();
        try {
            File file =new File(path);
            InputStream in =new FileInputStream(file);
            properties.load(in);
            map = new HashMap<String, String>((Map) properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;

    }
}

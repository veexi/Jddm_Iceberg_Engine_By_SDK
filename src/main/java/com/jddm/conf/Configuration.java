package com.jddm.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties; 

public class Configuration {
	
	private Properties pro;  
    private FileInputStream inputFile;  
    private FileOutputStream outputFile;  
 
    public Configuration(){  
         pro = new Properties();  
    }  
 
    /** 
     * <p>初始化Configuration类</p> 
     * @param filePath 要读取的配置文件的路径+名称 
     */  
    public Configuration(String filePath){  
        pro = new Properties();  
        try {  
            //读取属性文件  
            inputFile = new FileInputStream(filePath);  
            //装载文件  
            pro.load(inputFile);  
            inputFile.close();  
        } catch (FileNotFoundException e) {  
            System.out.println(" Read Property File Failed! Reason: The File Path is Wrong Or The File Does Not exist ... ");  
            e.printStackTrace();  
        } catch (IOException e) {  
            System.out.println(" Loading File Failed ! ");  
            e.printStackTrace();  
        }  
    }  
 
    /** 
     * <p>得到key值</p> 
     * @param key 取得其值的键 
     * @return 取得键值 
     */  
    public String getValue(String key){  
        if(pro.containsKey(key)){  
            String value = pro.getProperty(key);  
            return value;  
        }else{  
            return "";  
        }  
    }  
 
    /** 
     * <p>得到key值</p> 
     * @param filePath properties文件的路径+文件名 
     * @param key 取得其值的键 
     * @return 取得键值 
     */  
    public String getValue(String filePath, String key){  
        try {  
            String value = "";  
            inputFile = new FileInputStream(filePath);  
            pro.load(inputFile);  
            inputFile.close();  
            if(pro.contains(key)){  
                value = pro.getProperty(key);  
                return value;  
            }else{  
                return "";  
            }  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
            return "";  
        } catch (IOException e) {  
            e.printStackTrace();  
            return "";  
        } catch (Exception e) {  
            e.printStackTrace();  
            return "";  
        }  
    }  
 
    /**  
     * <p>清除properties文件中所有的key和其值</p> 
     */  
    public void clear(){  
        pro.clear();  
    }  
 
    /** 
     * <p>改变或添加一个key的值</p> 
     * 当key存在于properties文件中时该key的值被value所代替， 
     * 当key不存在时，该key的值是value 
     * @param key 要存入的键 
     * @param value 要存入的值 
     */  
    public void setValue(String key, String value){  
        pro.setProperty(key, value);  
    }  
 
    /**  
     * <p>将更改后的文件数据存入指定的文件中，该文件可以事先不存在</p> 
     * @param fileName 文件路径+文件名称 
     * @param description 对该文件的描述 
     */  
    public void saveFile(String fileName, String description){  
        try {  
            outputFile = new FileOutputStream(fileName);  
            pro.store(outputFile, description);  
            outputFile.close();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException ioe){  
            ioe.printStackTrace();  
        }  
    } 
 
    /** 
     * 测试方法 
     * @param args 
     */  
    public static void main(String[] args) {  
        Configuration conf = new Configuration("./config/config.properties");  
        String value = conf.getValue("LOCALIP");  
        System.out.println(value);  
    }  
}

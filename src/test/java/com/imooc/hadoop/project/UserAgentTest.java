package com.imooc.hadoop.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

/**
 * 测试UserAgentParser
 * @author mao
 *
 */
public class UserAgentTest {
	
	/**
	 * 单机版统计访问日志中的浏览器使用情况
	 * @throws IOException
	 */
	@Test
	public void testReadFile() throws IOException{
		String path = "";
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(new File(path)))
				);
		
		UserAgentParser userAgentParser = new UserAgentParser();
		String line = "";    //日志文件路径
		int sum = 0;  //统计日志总条数，防止条目丢失
		Map<String, Integer> browerMap = new HashMap<String,Integer>();   //保存浏览器出现的次数
		
		
		while(line != null){
			line = reader.readLine(); //一次读入一行数据	
			sum++;
			if(StringUtils.isNotBlank(line)){
				String source = line.substring(getCharPosition(line, "\"", 7)) + 1;
				UserAgent agent = userAgentParser.parse(source);
				String browser = agent.getBrowser();
				String engine = agent.getEngine();
				String engineVersion = agent.getEngineVersion();
				String os = agent.getOs();
				String platform = agent.getPlatform();
				
				Integer browerValue = browerMap.get(browser);
				if(browerValue != null){
					browerMap.put(browser,browerValue + 1);
				}else{
					browerMap.put(browser,1);
				}
				
				
				
				System.out.println(browser + "," + engine + "," + engineVersion + "," + os + "," + platform);
				
				
				
			}
		}
		System.out.println("records:" + sum);
	}
	
	/**
	 * 测试自定义方法
	 */
	@Test
	public void testGetCharPosition(){
		String str = "";
		int index = getCharPosition(str,"\"",7);
		System.out.println(index);
	}
	
	/**
	 * 获取字符串中指定字符的索引位置
	 */
	public int getCharPosition(String str, String operator , int index){
		Matcher slashMatcher = Pattern.compile(operator).matcher(str);
		int mIdx = 0;

		while(slashMatcher.find()){
			mIdx++;
			if(mIdx == index){
				break;
			}
		}
		return slashMatcher.start();
	}
	
	
	/**
	 * 
	 */
	
	@Test
	public void testUserAgentParser() {
		String source = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36";
		UserAgentParser userAgentParser = new UserAgentParser();
		UserAgent agent = userAgentParser.parse(source);
		String browser = agent.getBrowser();
		String engine = agent.getEngine();
		String engineVersion = agent.getEngineVersion();
		String os = agent.getOs();
		String platform = agent.getPlatform();
		System.out.println(browser + "," + engine + "," + engineVersion + "," + os + "," + platform);
	}

}

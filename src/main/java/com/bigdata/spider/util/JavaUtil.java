package com.bigdata.spider.util;

/**
 * Created by 11725 on 2017/7/1.
 */

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import org.json.JSONException;
import org.json.JSONObject;

public class JavaUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static String getSplitWords(String line){

        if (line == null || line.trim().length() == 0){
            return "";
        }

        StringBuffer splitedWord=new StringBuffer() ;

        StringReader sr=new StringReader(line);
        IKSegmenter ik=new IKSegmenter(sr, true);
        Lexeme lex;
        try {
            while((lex=ik.next())!=null){
                splitedWord.append(lex.getLexemeText()+" ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return splitedWord.toString();
    }

    public static String getJsonParse(String jsonStr){
        String jsonParsed="";
        try {
            JSONObject str = new JSONObject(jsonStr);
            jsonParsed=str.getString("content");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonParsed;
    }
    
    public static void main(String[] args) {
    	System.out.println("wahaha ");
    	System.out.println(getSplitWords("要实现这样的图像风格转换，通常需要一个包含成对图片的训练集。"));
	}

}

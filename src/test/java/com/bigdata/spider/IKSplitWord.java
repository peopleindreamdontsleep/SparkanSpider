package com.bigdata.spider;

import java.io.IOException;
import java.io.StringReader;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class IKSplitWord {
	
	public static void main(String[] args) {
		
		System.out.println(getSplitWords("要实现这样的图像风格转换，通常需要一个包含成对图片的训练集。"));
		
	}
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

}

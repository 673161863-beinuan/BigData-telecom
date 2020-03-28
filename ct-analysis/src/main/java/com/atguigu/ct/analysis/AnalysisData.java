package com.atguigu.ct.analysis;

import org.apache.hadoop.util.ToolRunner;

import com.atguigu.ct.analysis.tool.AnalysisBeanTool;
import com.atguigu.ct.analysis.tool.AnalysisTextTool;

public class AnalysisData {
	
	public static void main(String[] args) throws Exception {
		
		//int result = ToolRunner.run( new AnalysisTextTool(), args );
        int result = ToolRunner.run( new AnalysisBeanTool(), args );
	}

}

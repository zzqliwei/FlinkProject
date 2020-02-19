package com.westar.api.source;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

/**
 * 自定义的 Flink 的 Source
 *  从 gzip 文件中读取数据，然后模拟发送数据
 *  注意：通过实现 SourceFunction 接口实现的 Source 不支持并行
 */
public class GZIPFileSource implements SourceFunction<String> {
    //文件路径
    private String dataFilePath;
    public GZIPFileSource(String dataFilePath){
        this.dataFilePath = dataFilePath;
    }
    //随机发送数据
    private Random random = new Random();

    private InputStream inputStream;
    private BufferedReader reader;


    @Override
    public void run(SourceContext ctx) throws Exception {
        //首先读取gz文件
        inputStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;
        while ((line = reader.readLine()) != null){
            //模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            ctx.collect(line);
        }
        reader.close();
        reader = null;
        inputStream.close();
        inputStream = null;
    }

    @Override
    public void cancel() {//fink job 被取消的时候，被调用这个方法
        try{
            if(reader != null){
                reader.close();
            }
            if(null != inputStream){
                inputStream.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}

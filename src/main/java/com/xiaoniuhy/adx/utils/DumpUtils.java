package com.xiaoniuhy.adx.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import  com.xiaoniuhy.adp.pb.clickhouse.*;
import com.xiaoniuhy.adx.pb.adx.adpos_events.*;

public class DumpUtils {


    public static void main(String[] args){
        boolean all = true;
        if(args.length > 0){
            try
            {
                int read_size = 0;
                if(args.length > 1)
                {
                    read_size = Integer.parseInt(args[1]);
                }
                System.out.println("will read len:" + read_size);


                File file = new File(args[0]);
                FileInputStream fis = new FileInputStream(file);
                
    
                AdxAdposEvents event = AdxAdposEvents.parseDelimitedFrom(fis);
                int index = 0;
                while(event != null)
                {
                    System.out.println("*******index:"+index++ + "*************");
                    System.out.println(event);
                    event = AdxAdposEvents.parseDelimitedFrom(fis);
                    if(read_size != 0 && index > read_size){
                        break;
                    }
                }
                
            }
            catch(Exception e)
            {
                System.out.println(e);
            }

            
        }
        else{
            System.out.println("please input file path!");
        }
        
    }
}

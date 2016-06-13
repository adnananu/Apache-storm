package com.spnotes.storm.bolts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class DataProcessBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
            String sentence = input.getString(0);
            String ProductId = "";
            String EventType = "";
            String EventId = "";
            String createTimeStamp = "";
            String receiveTimeStamp = "";
            String TimeDifference = "";
            JSONParser parser = new JSONParser();
            try {
                JSONObject jsonObject = (JSONObject) parser.parse(sentence);
                EventType = (String) jsonObject.get("type");
                EventId = (String) jsonObject.get("event_id");
                receiveTimeStamp = String.valueOf(jsonObject.get("timestamp"));

                //getting next json Object for (device_id) from main json Object
                JSONObject ObjSecond = (JSONObject) jsonObject.get("device");
                ProductId = (String) ObjSecond.get("device_id");

                //getting next json Object for (create_timestamp) from main json Object
                ObjSecond = (JSONObject) jsonObject.get("time");
                createTimeStamp = String.valueOf(ObjSecond.get("create_timestamp"));

                TimeDifference = getTimeDifference(createTimeStamp,receiveTimeStamp);
                if(!TimeDifference.isEmpty())
                    TimeDifference = ProductId.concat(":").concat(TimeDifference);

                if (!ProductId.equals("") && EventType.equals("launch"))
                    collector.emit(new Values(ProductId,EventId,TimeDifference));
                else
                    collector.emit(new Values("",EventId,TimeDifference));

            } catch (ParseException e) {
                e.printStackTrace();
            }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ProductID","EventID","TimeDiff"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
    //get the time difference
    String getTimeDifference(String cts, String rts)
    {
        SimpleDateFormat sfd = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        Date cDate = new Date (Long.parseLong(cts));
        Date rDate = new Date (Long.parseLong(rts));
        long diff = 0;
        try {
            cDate = sfd.parse(sfd.format(cDate));
            rDate = sfd.parse(sfd.format(rDate));
            //in milliseconds
            diff =  cDate.getTime() - rDate.getTime();
            if(diff < 0){
                //diff = (diff)*(-1);
                return "";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
       return String.valueOf(diff/1000);
    }
}

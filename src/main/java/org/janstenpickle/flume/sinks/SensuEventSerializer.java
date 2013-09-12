package org.janstenpickle.flume.sinks;

import org.apache.flume.Event;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 05/09/2013
 * Time: 12:34
 * To change this template use File | Settings | File Templates.
 */
public class SensuEventSerializer {


    private static final Logger LOG = LoggerFactory
            .getLogger(SensuEventSerializer.class);

    private List<String> handlers;
    private String alertLevel;

    public static final String[] requiredHeaders = new String[]{
            "hostname",
            "timeStamp",
            "loggerName"
    };

    public SensuEventSerializer(List<String> handlers, String alertLevel) {
        this.handlers = handlers;
        this.alertLevel = alertLevel;

    }

    public byte[] getJsonByteData(Event event) throws SensuEventSerializationException {

        LOG.debug("Beginning serialization of event");

        Map<String, String> headers = event.getHeaders();
        validateRequiredKeys(headers);

        JSONObject sensuEvent = new JSONObject();
        JSONObject check = parseCheckInfo(event);
        sensuEvent.put("check", check);
        sensuEvent.put("client", headers.get("hostname"));
        sensuEvent.put("action", "create");
        sensuEvent.put("occurrences", 1);

        LOG.debug("Sensu event: " + sensuEvent.toJSONString());

        return sensuEvent.toJSONString().getBytes();
    }

    private int getStatus(String alertLevel) throws SensuEventSerializationException {
        switch (AlertLevelEnum.valueOf(alertLevel)) {
            case CRITICAL:
                return 2;
            case WARNING:
                return 1;
            case UNKNOWN:
                return 3;
            default:
                throw new SensuEventSerializationException("alertLevel must be one of CRITICAL,WARNING,UNKNOWN");
        }
    }

    private JSONObject parseCheckInfo(Event event) throws SensuEventSerializationException {
        Map<String, String> headers = event.getHeaders();

        JSONObject check = new JSONObject();
        JSONArray history = new JSONArray();

        history.add("0");
        history.add("0");

        StringBuilder output = new StringBuilder();
        output.append(headers.get("loggerName"));
        output.append(" - ");
        output.append(new String(event.getBody()));

        check.put("handlers", listToJsonArray(handlers));
        check.put("output", output.toString());
        check.put("status", getStatus(alertLevel));
        check.put("issued", convertTimestamp(headers.get("timeStamp")));
        check.put("executed", convertTimestamp(headers.get("timeStamp")));
        check.put("name", "flume_" + alertLevel);
        //check.put("duration", 0);
        //check.put("command", "n/a");
        check.put("interval", 1);
        check.put("flapping", false);
        check.put("history", history);

        return check;
    }

    private void validateRequiredKeys(Map<String, String> headers) throws SensuEventSerializationException {

        LOG.debug(headers.toString());

        for (int i = 0; i < requiredHeaders.length; i++) {
            String requiredHeader = requiredHeaders[i];
            if (!headers.containsKey(requiredHeader)) {
                throw new SensuEventSerializationException("Flume event headers do not contain required header " + requiredHeader);
            }
        }
    }

    private String convertTimestamp(String timestamp) {
        long ts = Long.parseLong(timestamp);
        ts = ts / 1000;
        return String.valueOf(ts);
    }

    private JSONArray listToJsonArray(List<String> list) {

        JSONArray jsonArray = new JSONArray();

        for (Iterator<String> listIterator = list.iterator(); listIterator.hasNext(); ) {
            String listItem = listIterator.next();

            jsonArray.add(listItem);
        }

        return jsonArray;
    }
}

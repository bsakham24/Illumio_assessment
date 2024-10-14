import java.io.*;
import java.util.*;

public class FlowLogParser {

    private static final Map<String, String> lookupTable = new HashMap<>();

    private static final Map<String, Integer> tagCounts = new HashMap<>();

    private static final Map<String, Integer> portProtocolCounts = new HashMap<>();

    public static void main(String[] args) {
        String lookupFile = "input/lookup_table.csv";
        String flowLogFile = "input/flow_logs.txt";
        String outputFile = "output/output_report.txt";

        loadLookupTable(lookupFile);

        parseFlowLogs(flowLogFile);

        writeOutput(outputFile);

        System.out.println("Output written to: " + outputFile);
    }

    private static void loadLookupTable(String lookupFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(lookupFile))) {
            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String dstPort = parts[0];
                String protocol = parts[1].toLowerCase();
                String tag = parts[2];
                lookupTable.put(dstPort + "," + protocol, tag);
            }
        } catch (IOException e) {
            System.err.println("Error reading lookup table: " + e.getMessage());
        }
    }

    private static void parseFlowLogs(String flowLogFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(flowLogFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\s+");

                if (parts.length < 7) {
                    System.err.println("Invalid flow log line (skipping): " + line);
                    continue;
                }

                String dstPort = parts[5];
                String protocolNum = parts[7];

                String protocolName = getProtocolName(protocolNum);
                String portProtocolKey = dstPort + "," + protocolName;

                String tag = lookupTable.getOrDefault(portProtocolKey, "Untagged");

                tagCounts.put(tag, tagCounts.getOrDefault(tag, 0) + 1);

                portProtocolCounts.put(portProtocolKey, portProtocolCounts.getOrDefault(portProtocolKey, 0) + 1);
            }
        } catch (IOException e) {
            System.err.println("Error reading flow log: " + e.getMessage());
        }
    }

    private static String getProtocolName(String protocolNum) {
        return switch (protocolNum) {
            case "6" -> "tcp";
            case "17" -> "udp";
            default -> "other";
        };
    }


    private static void writeOutput(String outputFile) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            bw.write("Tag Counts:\n");
            bw.write("Tag,Count\n");
            for (Map.Entry<String, Integer> entry : tagCounts.entrySet()) {
                bw.write(entry.getKey() + "," + entry.getValue() + "\n");
            }

            bw.write("\nPort/Protocol Combination Counts:\n");
            bw.write("Port,Protocol,Count\n");
            for (Map.Entry<String, Integer> entry : portProtocolCounts.entrySet()) {
                String[] portProtocol = entry.getKey().split(",");
                String port = portProtocol[0];
                String protocol = portProtocol[1];
                bw.write(port + "," + protocol + "," + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing output file: " + e.getMessage());
        }
    }
}

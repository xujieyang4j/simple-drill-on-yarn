package chapter12;

import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Drill on YARN Application Master
 * Created by 徐洁阳 on 2019-04-06.
 *
 * @author 徐洁阳
 */
public class DrillOnYarnAM {
    private static AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
    private static NMClientAsync nmClientAsync;
    private static YarnConfiguration yarnConfig;
    private static Properties appConfig;
    private static YarnClient yarnClient;

    public static void main(String[] args) {
        Map<String, String> sysEnvironment = System.getenv();
        try {
            initAppConfig(sysEnvironment);

            yarnConfig = new YarnConfiguration();

            AMRMCallbackHandler amrmCallbackHandler = initAMRMClientAsync();

            initNMClientAsync(amrmCallbackHandler);

            Resource capability = createResource();

            Priority priority = createPriority();

            registerAM();

            yarnClient = initYarnClient();

            List<NodeReport> nodeReports = getNodeReports(yarnClient);

            int containers = Integer.parseInt(appConfig.getProperty("containers.num"));
            System.out.println("containers.num is " + containers);
            if (containers > nodeReports.size()) {
                throw new RuntimeException("containers num can not more than node num");
            }

            nodeReports = shuffleNodeReports(nodeReports, containers);

            addContainerRequest(capability, priority, nodeReports, amrmCallbackHandler);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (nmClientAsync != null) {
                    nmClientAsync.close();
                }
                if (amrmClientAsync != null) {
                    amrmClientAsync.close();
                }
                if (yarnClient != null) {
                    yarnClient.close();
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }

    private static void addContainerRequest(Resource capability, Priority priority, List<NodeReport> nodeReports,
                                            AMRMCallbackHandler amrmCallbackHandler) {
        for (NodeReport containerReport : nodeReports) {
            AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability,
                    new String[] {containerReport.getNodeId().getHost()},
                    null, priority, false);
            amrmCallbackHandler.getContainerRequests().add(containerRequest);
            amrmClientAsync.addContainerRequest(containerRequest);
        }
    }

    private static List<NodeReport> shuffleNodeReports(List<NodeReport> nodeReports, int containers) {
        Collections.shuffle(nodeReports);
        return new ArrayList<>(nodeReports.subList(0, containers));
    }

    private static List<NodeReport> getNodeReports(YarnClient yarnClient) {
        List<NodeReport> nodeReports;
        try {
            nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
            System.out.println("running node reports size is " + nodeReports.size());
            System.out.println("running node reports is " + nodeReports);
        } catch (YarnException | IOException e) {
            throw new RuntimeException("get running node report error", e);
        }
        return nodeReports;
    }

    private static YarnClient initYarnClient() {
        System.out.println("start init yarn client");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();
        System.out.println("end init yarn client");
        return yarnClient;
    }

    private static void registerAM() {
        try {
            System.out.println("start register application master");
            String hostname = InetAddress.getLocalHost().getHostName();
            amrmClientAsync.registerApplicationMaster(hostname, 0, "");
            System.out.println("end register application master");
        } catch (YarnException | IOException e) {
            throw new RuntimeException("register application master", e);
        }
    }

    private static Priority createPriority() {
        return Priority.newInstance(Integer.parseInt(appConfig.getProperty("container.priority")));
    }

    private static Resource createResource() {
        return Resource.newInstance(Integer.parseInt(appConfig.getProperty("container.memory")),
                Integer.parseInt(appConfig.getProperty("container.vCores")));
    }

    private static void initNMClientAsync(AMRMCallbackHandler amrmCallbackHandler) {
        System.out.println("start init async nm client");
        nmClientAsync = NMClientAsync.createNMClientAsync(new NMCallbackHandler());
        nmClientAsync.init(yarnConfig);
        nmClientAsync.start();
        amrmCallbackHandler.setNmClientAsync(nmClientAsync);
        System.out.println("end init async nm client");
    }

    private static AMRMCallbackHandler initAMRMClientAsync() {
        System.out.println("start init async amrm client");
        AMRMCallbackHandler amrmCallbackHandler = new AMRMCallbackHandler();
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(5000, amrmCallbackHandler);
        amrmClientAsync.init(yarnConfig);
        amrmClientAsync.start();
        amrmCallbackHandler.setYarnConfig(yarnConfig);
        amrmCallbackHandler.setAppConfig(appConfig);
        amrmCallbackHandler.setAmrmClientAsync(amrmClientAsync);
        System.out.println("end init async amrm client");
        return amrmCallbackHandler;
    }

    private static void initAppConfig(Map<String, String> sysEnvironment) throws IOException {
        appConfig = new Properties();
        String configBase64Binary = sysEnvironment.get("DRILL_ON_YARN_CONFIG");
        appConfig.load(new InputStreamReader(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(configBase64Binary)),
                StandardCharsets.UTF_8.name()));
        System.out.println("app config is " + appConfig);
    }
}

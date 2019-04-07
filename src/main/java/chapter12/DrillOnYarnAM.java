package chapter12;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
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
    private static Set<ContainerId> allocatedContainerIds = new LinkedHashSet<>();
    private static Set<AMRMClient.ContainerRequest> allocatedContainerRequests = new LinkedHashSet<>();
    private static Set<String> allocatedContainersIdAndHost = new LinkedHashSet<>();
    private static Set<ContainerId> completedContainerIds = new LinkedHashSet<>();
    private static YarnConfiguration yarnConfig;
    private static Properties appConfig;
    private static YarnClient yarnClient;

    public static void main(String[] args) {
        try {
            Map<String, String> sysEnvironment = System.getenv();

            initAppConfig(sysEnvironment);

            yarnConfig = new YarnConfiguration();

            initAMRMClientAsync();

            initNMClientAsync();

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

            addContainerRequest(capability, priority, nodeReports);
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

    private static class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @Override
        public void onContainersAllocated(List<Container> containers) {
            System.out.println("currently allocated containers size is " + containers.size());
            System.out.println("currently allocated containers is " + containers);
            for (Container container : containers) {
                ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);

                setContainerEnvironment(containerContext);

                setContainerResource(containerContext);

                setContainerCommand(containerContext);

                nmClientAsync.startContainerAsync(container, containerContext);

                allocatedContainersIdAndHost.add(
                        container.getId().getContainerId() + "#" + container.getNodeId().getHost());
                allocatedContainerIds.add(container.getId());
            }

            if (containers.size() > 0) {
                logContainerAllocate();
            }

            if (allocatedContainerIds.size() == Integer.parseInt(appConfig.getProperty("containers.num"))) {
                removeContainerRequest();
            }
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            System.out.println("completed containers statuses size is " + statuses.size());
            System.out.println("completed containers statuses is " + statuses);
            for (ContainerStatus containerStatus : statuses) {
                if (!completedContainerIds.contains(containerStatus.getContainerId())) {
                    ContainerId containerId = containerStatus.getContainerId();
                    completedContainerIds.add(containerId);

                    allocatedContainerIds.remove(containerId);

                    logContainerCompleteInfo(containerStatus, containerId);
                }

            }

            restartContainerIfNeed();
        }

        @Override
        public void onShutdownRequest() {

        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void onError(Throwable e) {

        }
    }

    private static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        @Override
        public void onContainerStopped(ContainerId containerId) {

        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {

        }

    }

    private static void removeContainerRequest() {
        System.out.println("all container request finish, remove other container request");
        for (AMRMClient.ContainerRequest containerRequest : allocatedContainerRequests) {
            amrmClientAsync.removeContainerRequest(containerRequest);
        }
        allocatedContainerRequests.clear();
    }

    private static void logContainerAllocate() {
        int needed = Integer.parseInt(appConfig.getProperty("containers.num")) - allocatedContainerIds
                .size();
        if (needed > 0) {
            System.out.println(allocatedContainerIds.size() + " containers allocated, " + needed + " remaining");
        } else {
            System.out.println("Fully allocated " + allocatedContainerIds.size() + " containers");
        }
    }

    private static void setContainerCommand(ContainerLaunchContext containerContext) {
        System.out.println("start set container command");
        List<String> commands = new ArrayList<>();
        commands.add(ApplicationConstants.Environment.SHELL.$$());
        commands.add(FilenameUtils.getName(appConfig.getProperty("drill.path")) + "/" + appConfig.getProperty("drill.archive.name") + "/bin/drillbit.sh run");
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);
        StringBuilder containerCommand = new StringBuilder();
        for (String str : commands) {
            containerCommand.append(str).append(" ");
        }
        containerCommand.setLength(containerCommand.length() - " ".length());
        containerContext.setCommands(Collections.singletonList(containerCommand.toString()));
        System.out.println("end set container command");
    }

    private static void setContainerResource(ContainerLaunchContext containerContext) {
        System.out.println("start set container resources");
        Map<String, LocalResource> containerResources = new LinkedHashMap<>();
        LocalResource drillArchive = Records.newRecord(LocalResource.class);
        String drill = appConfig.getProperty("fs.upload.dir") + FilenameUtils.getName(appConfig.getProperty("drill.path"));

        Path path = new Path(drill);
        FileStatus fileStatus;
        try {
            fileStatus = FileSystem.get(yarnConfig).getFileStatus(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        drillArchive.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()));
        drillArchive.setSize(fileStatus.getLen());
        drillArchive.setTimestamp(fileStatus.getModificationTime());
        drillArchive.setType(LocalResourceType.ARCHIVE);
        drillArchive.setVisibility(LocalResourceVisibility.PUBLIC);
        containerResources.put(fileStatus.getPath().getName(), drillArchive);
        containerContext.setLocalResources(containerResources);
        System.out.println("end set container resources");
    }

    private static void setContainerEnvironment(ContainerLaunchContext containerContext) {
        System.out.println("start set container environment");
        Map<String, String> containerEnvironment = new LinkedHashMap<>();
        // add Hadoop Classpath
        for (String classpath : yarnConfig.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(containerEnvironment, ApplicationConstants.Environment.CLASSPATH.name(),
                    classpath.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        Apps.addToEnvironment(containerEnvironment, ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", ApplicationConstants.CLASS_PATH_SEPARATOR);
        containerContext.setEnvironment(containerEnvironment);
        System.out.println("end set container environment");
    }

    private static void restartContainerIfNeed() {
        System.out.println("completed container ids is " + completedContainerIds);
        System.out.println("allocated container ids is " + allocatedContainerIds);
        if (CollectionUtils.isNotEmpty(completedContainerIds)) {
            Set<String> restartContainersHost = new LinkedHashSet<>();
            for (String allocatedContainerHost : allocatedContainersIdAndHost) {
                for (ContainerId completedContainerId : completedContainerIds) {
                    String containerId = allocatedContainerHost.split("#")[0];
                    String host = allocatedContainerHost.split("#")[1];
                    if (Long.parseLong(containerId) == completedContainerId.getContainerId()) {
                        restartContainersHost.add(host);
                    }
                }
            }
            System.out.println("restart containers host is " + restartContainersHost);
            for (String host : restartContainersHost) {
                AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(
                        createResource(), new String[] {host}, null, createPriority(), false);
                allocatedContainerRequests.add(containerRequest);
                amrmClientAsync.addContainerRequest(containerRequest);
            }
            completedContainerIds.clear();
        }
    }

    private static void logContainerCompleteInfo(ContainerStatus containerStatus, ContainerId containerId) {
        boolean containerSuccessful = false;
        switch (containerStatus.getExitStatus()) {
            case ContainerExitStatus.SUCCESS:
                System.out.println("container exit status is " + containerStatus.getExitStatus() + ", container " + containerId + " finished successfully...");
                containerSuccessful = true;
                break;
            case ContainerExitStatus.ABORTED:
                System.err.println("container exit status is " + containerStatus.getExitStatus() + ", container " + containerId + " aborted...");
                break;
            case ContainerExitStatus.DISKS_FAILED:
                System.err.println("container exit status is " + containerStatus.getExitStatus() + ", container " + containerId + " ran out of disk...");
                break;
            case ContainerExitStatus.PREEMPTED:
                System.err.println("container exit status is " + containerStatus.getExitStatus() + ", container " + containerId + " preempted..");
                break;
            default:
                System.err.println("container exit status is " + containerStatus.getExitStatus() + ", container " + containerId + " exited with an invalid/unknown exit code...");
        }

        if (!containerSuccessful) {
            System.err.println("luster has not completed successfully...");
        }
    }

    private static void addContainerRequest(Resource capability, Priority priority, List<NodeReport> nodeReports) {
        for (NodeReport containerReport : nodeReports) {
            AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability,
                    new String[] {containerReport.getNodeId().getHost()},
                    null, priority, false);
            allocatedContainerRequests.add(containerRequest);
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

    private static void initNMClientAsync() {
        System.out.println("start init async nm client");
        nmClientAsync = NMClientAsync.createNMClientAsync(new NMCallbackHandler());
        nmClientAsync.init(yarnConfig);
        nmClientAsync.start();
        System.out.println("end init async nm client");
    }

    private static void initAMRMClientAsync() {
        System.out.println("start init async amrm client");
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(5000, new AMRMCallbackHandler());
        amrmClientAsync.init(yarnConfig);
        amrmClientAsync.start();
        System.out.println("end init async amrm client");
    }

    private static void initAppConfig(Map<String, String> sysEnvironment) {
        appConfig = new Properties();
        try {
            String configBase64Binary = sysEnvironment.get("DRILL_ON_YARN_CONFIG");
            appConfig.load(new InputStreamReader(new ByteArrayInputStream(DatatypeConverter.parseBase64Binary(configBase64Binary)),
                    StandardCharsets.UTF_8.name()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("app config is " + appConfig);
    }
}

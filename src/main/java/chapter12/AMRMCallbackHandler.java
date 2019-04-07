package chapter12;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.*;

/**
 * Created by 徐洁阳 on 2019-04-07.
 *
 * @author 徐洁阳
 */
public class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private YarnConfiguration yarnConfig;
    private Properties appConfig;
    private AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
    private NMClientAsync nmClientAsync;
    private Set<AMRMClient.ContainerRequest> containerRequests = new HashSet<>();
    private Set<ContainerInfo> allocatedContainerInfo = new HashSet<>();
    private Set<ContainerId> completedContainerIds = new HashSet<>();

    public void setYarnConfig(YarnConfiguration yarnConfig) {
        this.yarnConfig = yarnConfig;
    }

    public void setAppConfig(Properties appConfig) {
        this.appConfig = appConfig;
    }

    public void setAmrmClientAsync(AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync) {
        this.amrmClientAsync = amrmClientAsync;
    }

    public void setNmClientAsync(NMClientAsync nmClientAsync) {
        this.nmClientAsync = nmClientAsync;
    }

    public Set<AMRMClient.ContainerRequest> getContainerRequests() {
        return containerRequests;
    }

    private class ContainerInfo {
        private ContainerId containerId;
        private String host;

        public ContainerInfo(ContainerId containerId, String host) {
            this.containerId = containerId;
            this.host = host;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ContainerInfo that = (ContainerInfo) o;
            return Objects.equals(containerId, that.containerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(containerId);
        }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        System.out.println("currently allocated containers is " + containers + ", num is " + containers.size());
        for (Container container : containers) {
            ContainerLaunchContext containerContext = Records.newRecord(ContainerLaunchContext.class);

            setContainerEnvironment(containerContext);

            setContainerResource(containerContext);

            setContainerCommand(containerContext);

            nmClientAsync.startContainerAsync(container, containerContext);

            allocatedContainerInfo.add(new ContainerInfo(container.getId(), container.getNodeId().getHost()));
        }

        logContainerAllocate();

        if (allocatedContainerInfo.size() == Integer.parseInt(appConfig.getProperty("containers.num"))) {
            removeContainerRequest();
        }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        System.out.println("completed containers statuses is " + statuses + ", size is " + statuses.size());
        for (ContainerStatus status : statuses) {
            if (!completedContainerIds.contains(status.getContainerId())) {
                ContainerId containerId = status.getContainerId();
                completedContainerIds.add(containerId);

                allocatedContainerInfo.remove(new ContainerInfo(containerId, ""));

                logContainerCompleteInfo(status, containerId);
            }
        }
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

    private void setContainerEnvironment(ContainerLaunchContext containerContext) {
        System.out.println("start set container environment");
        Map<String, String> containerEnvironment = YarnUtil.buildCommonEnvironment(yarnConfig);
        containerContext.setEnvironment(containerEnvironment);
        System.out.println("end set container environment");
    }

    private void setContainerResource(ContainerLaunchContext containerContext) {
        System.out.println("start set container resources");
        String drill = appConfig.getProperty("fs.upload.dir") + FilenameUtils.getName(appConfig.getProperty("drill.path"));
        Path path = new Path(drill);
        FileStatus fileStatus;
        try {
            fileStatus = FileSystem.get(yarnConfig).getFileStatus(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        containerContext.setLocalResources(YarnUtil.buildResource(fileStatus, LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        System.out.println("end set container resources");
    }

    private void setContainerCommand(ContainerLaunchContext containerContext) {
        System.out.println("start set container command");
        containerContext.setCommands(YarnUtil.buildCommand(FilenameUtils.getName(appConfig.getProperty("drill.path")) +
                "/" + appConfig.getProperty("drill.archive.name") + "/bin/drillbit.sh run"));
        System.out.println("end set container command");
    }

    private void logContainerAllocate() {
        int needed = Integer.parseInt(appConfig.getProperty("containers.num")) - allocatedContainerInfo.size();
        if (needed > 0) {
            System.out.println(allocatedContainerInfo.size() + " containers allocated, " + needed + " remaining");
        } else {
            System.out.println("fully allocated " + allocatedContainerInfo.size() + " containers");
        }
    }

    private void removeContainerRequest() {
        System.out.println("all container request finish, remove other container request");
        for (AMRMClient.ContainerRequest containerRequest : containerRequests) {
            amrmClientAsync.removeContainerRequest(containerRequest);
        }
        containerRequests.clear();
    }

    private void logContainerCompleteInfo(ContainerStatus containerStatus, ContainerId containerId) {
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
            System.err.println("cluster has not completed successfully...");
        }
    }
}

package chapter12;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by 徐洁阳 on 2019-04-07.
 *
 * @author 徐洁阳
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

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

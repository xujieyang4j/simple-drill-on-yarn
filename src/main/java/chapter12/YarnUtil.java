package chapter12;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by 徐洁阳 on 2019-04-07.
 *
 * @author 徐洁阳
 */
public class YarnUtil {
    public static List<String> buildCommand(String command) {
        List<String> commands = new ArrayList<>();
        commands.add(ApplicationConstants.Environment.SHELL.$$());
        commands.add(command);
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);
        StringBuilder builder = new StringBuilder();
        for (String str : commands) {
            builder.append(str).append(" ");
        }
        builder.setLength(builder.length() - " ".length());
        return Collections.singletonList(builder.toString());
    }

    public static Map<String, LocalResource> buildResource(FileStatus fileStatus, LocalResourceType type,
                                                           LocalResourceVisibility visibility) {
        Map<String, LocalResource> localResources = new LinkedHashMap<>();
        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(fileStatus.getPath()));
        localResource.setSize(fileStatus.getLen());
        localResource.setTimestamp(fileStatus.getModificationTime());
        localResource.setType(type);
        localResource.setVisibility(visibility);
        localResources.put(fileStatus.getPath().getName(), localResource);
        return localResources;
    }

    public static Map<String, String> buildCommonEnvironment(Configuration config) {
        Map<String, String> environment = new LinkedHashMap<>();
        // add Hadoop Classpath
        for (String classpath : config.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(),
                    classpath.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        Apps.addToEnvironment(environment, ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", ApplicationConstants.CLASS_PATH_SEPARATOR);
        return environment;
    }

    public static YarnClient initYarnClient(Configuration config) {
        YarnClient client = YarnClient.createYarnClient();
        client.init(config);
        client.start();
        return client;
    }

    public static YarnClientApplication createYarnApplication(YarnClient client) {
        YarnClientApplication application;
        try {
            application = client.createApplication();
        } catch (YarnException | IOException e) {
            throw new RuntimeException("create yarn application error", e);
        }
        return application;
    }


    public static ApplicationSubmissionContext createYarnApplicationContext(YarnClientApplication application,
                                                                            String applicationName, String queue,
                                                                            Integer priority, Integer memory, Integer vCore) {
        ApplicationSubmissionContext applicationContext = application.getApplicationSubmissionContext();
        applicationContext.setApplicationId(application.getNewApplicationResponse().getApplicationId());
        applicationContext.setApplicationName(applicationName);
        applicationContext.setQueue(queue);
        applicationContext.setPriority(Priority.newInstance(priority));
        applicationContext.setResource(Resource.newInstance(memory, vCore));
        return applicationContext;
    }

    public static void submitApplication(YarnClient client, ApplicationSubmissionContext applicationContext) {
        try {
            client.submitApplication(applicationContext);
        } catch (YarnException | IOException e) {
            throw new RuntimeException("submit application error", e);
        }
    }
}

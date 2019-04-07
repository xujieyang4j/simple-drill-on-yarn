package chapter12;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Drill on YARN客户端
 * Created by 徐洁阳 on 2019-04-06.
 *
 * @author 徐洁阳
 */
public class DrillOnYarnClient extends Configured implements Tool {
    private YarnClient yarnClient;
    private Configuration yarnConfig;

    public static void main(String[] args) {
        int status = -1;
        try {
            status = ToolRunner.run(new DrillOnYarnClient(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }


    @Override
    public int run(String[] args) {
        if (ArrayUtils.isEmpty(args)) {
            displayHelpInfo();
            return 0;
        }

        // cmd必须是：start、stop、status、resize、help
        String command = args[0];

        // 加载配置：启动命令传入的配置&默认的配置
        Properties config = loadConfig(args);

        switch (command) {
            case "start": {
                start(config);
                break;
            }
            case "stop": {
                break;
            }
            case "resize": {
                break;
            }
            case "status": {
                break;
            }
            case "help": {
                break;
            }
            default: {

            }
        }
        return 0;
    }

    private void start(Properties config) {
        try {
            FileStatus fileStatus = upload(config);

            yarnConfig = new YarnConfiguration(getConf());

            yarnClient = initYarnClient();

            YarnClientApplication application = createYarnApplication(yarnClient);

            ApplicationSubmissionContext applicationContext = createApplicationContext(config, application);

            ContainerLaunchContext amContainer = createAMContainer(fileStatus, config);

            applicationContext.setAMContainerSpec(amContainer);

            submitApplication(yarnClient, applicationContext);

            ApplicationId applicationId = applicationContext.getApplicationId();
            System.out.println("application id is  " + applicationId);

            reportApplication(yarnClient, applicationId);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (yarnClient != null) {
                    yarnClient.close();
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

    }

    private FileStatus upload(Properties config) {
        System.out.println("start upload Drill to HDFS");
        String drillPath = config.getProperty("drill.path");
        System.out.println("drill path is " + drillPath);
        File drill = new File(drillPath);
        Path path = new Path(config.getProperty("fs.upload.dir") + drill.getName());
        FileStatus drillFileStatus;
        try {
            FileSystem fileSystem = FileSystem.get(getConf());
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
            FileUtil.copy(drill, fileSystem, path, false, getConf());
            drillFileStatus = fileSystem.getFileStatus(path);
            System.out.println("Uploaded " + drillPath + " to HDFS at " + drillFileStatus.getPath() + " success");
        } catch (IOException e) {
            throw new RuntimeException("upload drill to HDFS error", e);
        }
        System.out.println("end upload Drill to HDFS");
        return drillFileStatus;
    }

    private YarnClient initYarnClient() {
        System.out.println("start create yarn yarnClient");
        YarnClient client = YarnUtil.initYarnClient(yarnConfig);
        System.out.println("end create yarn yarnClient");
        return client;
    }

    private YarnClientApplication createYarnApplication(YarnClient client) {
        System.out.println("start create application");
        YarnClientApplication application = YarnUtil.createYarnApplication(client);
        System.out.println("end create application");
        return application;
    }

    private ApplicationSubmissionContext createApplicationContext(Properties config, YarnClientApplication application) {
        System.out.println("start create application context");
        ApplicationSubmissionContext applicationContext = YarnUtil.createYarnApplicationContext(application, config.getProperty("app.name"),
                config.getProperty("app.queue"), Integer.parseInt(config.getProperty("app.priority")),
                Integer.parseInt(config.getProperty("am.memory")), Integer.parseInt(config.getProperty("am.vCores")));
        System.out.println("end create application context");
        return applicationContext;
    }

    private ContainerLaunchContext createAMContainer(FileStatus fileStatus, Properties config) {
        System.out.println("start create am container");
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        setAMContainerResources(amContainer, fileStatus);

        setAMContainerEnvironment(amContainer, yarnConfig, config);

        setAMContainerCommand(amContainer, fileStatus, config);
        System.out.println("end create am container");
        return amContainer;
    }

    private void setAMContainerResources(ContainerLaunchContext amContainer, FileStatus fileStatus) {
        System.out.println("start set am container resources");
        amContainer.setLocalResources(YarnUtil.buildResource(fileStatus, LocalResourceType.ARCHIVE, LocalResourceVisibility.PUBLIC));
        System.out.println("emd set am container resources");
    }

    private void setAMContainerCommand(ContainerLaunchContext amContainer, FileStatus drillFileStatus, Properties config) {
        System.out.println("start set am container command");
        amContainer.setCommands(YarnUtil.buildCommand(drillFileStatus.getPath().getName() + "/" +
                config.getProperty("drill.archive.name") + "/bin/drill-am.sh"));
        System.out.println("end set am container command");
    }

    private void setAMContainerEnvironment(ContainerLaunchContext amContainer, Configuration yarnConfig, Properties config) {
        System.out.println("start set am container environment");
        Map<String, String> amEnvironment = YarnUtil.buildCommonEnvironment(yarnConfig);
        // DRILL_ON_YARN_CONFIG -> config
        try {
            StringWriter sw = new StringWriter();
            config.store(sw, "");
            String configBase64Binary = DatatypeConverter.printBase64Binary(sw.toString().getBytes(StandardCharsets.UTF_8.name()));
            Apps.addToEnvironment(amEnvironment, "DRILL_ON_YARN_CONFIG", configBase64Binary,
                    ApplicationConstants.CLASS_PATH_SEPARATOR);
        } catch (IOException e) {
            throw new RuntimeException("Set am container environment error", e);
        }
        amContainer.setEnvironment(amEnvironment);
        System.out.println("end set am container environment");
    }

    private void reportApplication(YarnClient client, ApplicationId applicationId) {
        ApplicationReport report;
        try {
            report = client.getApplicationReport(applicationId);
        } catch (YarnException | IOException e) {
            throw new RuntimeException(e);
        }
        YarnApplicationState applicationState = report.getYarnApplicationState();
        int i = 0;
        while (true) {
            if (i == 60) {
                break;
            }
            if (YarnApplicationState.KILLED.equals(applicationState)) {
                System.out.println("application " + applicationId + " killed");
                break;
            }
            if (YarnApplicationState.FAILED.equals(applicationState)) {
                System.out.println("application " + applicationId + " failed");
                break;
            }
            if (YarnApplicationState.FINISHED.equals(applicationState)) {
                System.out.println("application " + applicationId + " finished");
                break;
            }
            System.out.println("application " + applicationId + " " + applicationState.name().toLowerCase());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }
        System.out.println("application " + applicationId + " " + applicationState.name().toLowerCase() + ", tracking url " + report.getTrackingUrl());
    }

    private void closeYarnClient(YarnClient client) {
        try {
            System.out.println("start close yarn yarnClient");
            client.close();
            System.out.println("end close yarn yarnClient");
        } catch (IOException e) {
            throw new RuntimeException("close yarn yarnClient error", e);
        }
    }

    private void submitApplication(YarnClient client, ApplicationSubmissionContext applicationContext) {
        System.out.println("start submit application");
        YarnUtil.submitApplication(client, applicationContext);
        System.out.println("end submit application");
    }

    private static Properties loadConfig(String[] args) {
        Properties config = new Properties();
        try {
            config.load(DrillOnYarnClient.class.getResourceAsStream("config.properties"));
            for (int i = 1; i < args.length; i++) {
                String configArg = args[i];
                String[] configArgs = configArg.split("=");
                String key = configArgs[0];
                String value = configArgs[1];
                config.setProperty(key, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("load config error", e);
        }
        return config;
    }

    private static void displayHelpInfo() {
        StringBuilder help = new StringBuilder();
        try {
            List<String> helpContent = IOUtils.readLines(DrillOnYarnClient.class.getResourceAsStream("help.txt"), "UTF-8");
            for (String string : helpContent) {
                help.append(string).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.print(help);
    }
}

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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Drill on YARN客户端
 * Created by 徐洁阳 on 2019-04-06.
 *
 * @author 徐洁阳
 */
public class DrillOnYarnClient extends Configured implements Tool {
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
        FileStatus drillFileStatus = upload(config);

        Configuration yarnConfig = new YarnConfiguration(getConf());

        YarnClient client = createYarnClient(yarnConfig);

        YarnClientApplication application = createApplication(client);

        ApplicationSubmissionContext applicationContext = createApplicationContext(config, application);

        ContainerLaunchContext amContainer = createAMContainer(drillFileStatus, yarnConfig, config);

        applicationContext.setAMContainerSpec(amContainer);
        submitApplication(client, applicationContext);
        ApplicationId applicationId = applicationContext.getApplicationId();
        System.out.println("application id is  " + applicationId);

        try {
            reportApplication(config, client, applicationId);
        } catch (YarnException | IOException e) {
            throw new RuntimeException("get application report error", e);
        } finally {
            closeYarnClient(client);
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

    private YarnClient createYarnClient(Configuration yarnConfig) {
        System.out.println("start create yarn client");
        YarnClient client = YarnClient.createYarnClient();
        client.init(yarnConfig);
        client.start();
        System.out.println("end create yarn client");
        return client;
    }

    private YarnClientApplication createApplication(YarnClient client) {
        System.out.println("start create application");
        YarnClientApplication app;
        try {
            app = client.createApplication();
        } catch (YarnException | IOException e) {
            throw new RuntimeException("error createApplication", e);
        }
        System.out.println("end create application");
        return app;
    }

    private ApplicationSubmissionContext createApplicationContext(Properties config, YarnClientApplication application) {
        System.out.println("start create application context");
        ApplicationSubmissionContext applicationContext = application.getApplicationSubmissionContext();
        applicationContext.setApplicationId(application.getNewApplicationResponse().getApplicationId());
        applicationContext.setApplicationName(config.getProperty("app.name"));
        applicationContext.setQueue(config.getProperty("app.queue"));
        applicationContext.setPriority(Priority.newInstance(Integer.parseInt(config.getProperty("app.priority"))));
        applicationContext.setResource(Resource.newInstance(Integer.parseInt(config.getProperty("am.memory")),
                Integer.parseInt(config.getProperty("am.vCores"))));
        System.out.println("end create application context");
        return applicationContext;
    }

    private ContainerLaunchContext createAMContainer(FileStatus drillFileStatus, Configuration yarnConfig, Properties config) {
        System.out.println("start create am container");
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        setAMContainerResources(amContainer, drillFileStatus);

        setAMContainerEnvironment(amContainer, yarnConfig, config);

        setAMContainerCommand(amContainer, drillFileStatus, config);
        System.out.println("end create am container");
        return amContainer;
    }

    private void setAMContainerResources(ContainerLaunchContext amContainer, FileStatus drillFileStatus) {
        System.out.println("start set am container resources");
        Map<String, LocalResource> amLocalResources = new LinkedHashMap<>();
        LocalResource drill = Records.newRecord(LocalResource.class);
        drill.setResource(ConverterUtils.getYarnUrlFromPath(drillFileStatus.getPath()));
        drill.setSize(drillFileStatus.getLen());
        drill.setTimestamp(drillFileStatus.getModificationTime());
        drill.setType(LocalResourceType.ARCHIVE);
        drill.setVisibility(LocalResourceVisibility.PUBLIC);
        amLocalResources.put(drillFileStatus.getPath().getName(), drill);
        amContainer.setLocalResources(amLocalResources);
        System.out.println("emd set am container resources");
    }

    private void setAMContainerCommand(ContainerLaunchContext amContainer, FileStatus drillFileStatus, Properties config) {
        System.out.println("start set am container command");
        // 设置command
        List<String> commands = new ArrayList<>();
        commands.add(ApplicationConstants.Environment.SHELL.$$());
        commands.add(drillFileStatus.getPath().getName() + "/" + config.getProperty("drill.archive.name") + "/bin/drill-am.sh");
        commands.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDOUT);
        commands.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/" + ApplicationConstants.STDERR);
        StringBuilder amCommand = new StringBuilder();
        for (String str : commands) {
            amCommand.append(str).append(" ");
        }
        amCommand.setLength(amCommand.length() - " ".length());
        amContainer.setCommands(Collections.singletonList(amCommand.toString()));
        System.out.println("end set am container command");
    }

    private void setAMContainerEnvironment(ContainerLaunchContext amContainer, Configuration yarnConfig, Properties config) {
        System.out.println("start set am container environment");
        Map<String, String> amEnvironment = new LinkedHashMap<>();
        // add Hadoop Classpath
        for (String classpath : yarnConfig.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(amEnvironment, ApplicationConstants.Environment.CLASSPATH.name(),
                    classpath.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
        }
        Apps.addToEnvironment(amEnvironment, ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", ApplicationConstants.CLASS_PATH_SEPARATOR);

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

    private void reportApplication(Properties config, YarnClient client, ApplicationId applicationId) throws YarnException, IOException {
        int i = 0;
        while(true) {
            if (i == 180) {
                break;
            }
            ApplicationReport report = client.getApplicationReport(applicationId);
            System.out.println(String.format("Launched a %d %s Drill-On-YARN cluster [%s@%s] at %tc",
                    Integer.parseInt(config.getProperty("containers.num")),
                    (Integer.parseInt(config.getProperty("containers.num")) > 1 ? "nodes" : "node"),
                    applicationId, report.getTrackingUrl(),
                    report.getStartTime()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            i++;
        }

    }

    private void closeYarnClient(YarnClient client) {
        try {
            System.out.println("start close yarn client");
            client.close();
            System.out.println("end close yarn client");
        } catch (IOException e) {
            throw new RuntimeException("close yarn client error", e);
        }
    }

    private void submitApplication(YarnClient client, ApplicationSubmissionContext applicationContext) {
        System.out.println("start submit application");
        try {
            client.submitApplication(applicationContext);
        } catch (YarnException | IOException e) {
            throw new RuntimeException("submit application error", e);
        }
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

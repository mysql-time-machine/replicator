package com.booking.replication.commons.services.containers.mysql;

import com.booking.replication.commons.conf.MySQLConfiguration;
import com.booking.replication.commons.services.containers.TestContainer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class MySQLContainer extends GenericContainer<MySQLContainer> implements TestContainer<MySQLContainer> {
    private static Logger LOG = LogManager.getLogger(MySQLContainer.class);

    private static final String IMAGE = "mysql";
    private static final String TAG = "5.6.38";

    private static final String DOCKER_IMAGE_KEY = "docker.image.mysql";
    private static final String ROOT_PASSWORD_KEY = "MYSQL_ROOT_PASSWORD";
    private static final String DATABASE_KEY = "MYSQL_DATABASE";
    private static final String USER_KEY = "MYSQL_USER";
    private static final String PASSWORD_KEY = "MYSQL_PASSWORD";
    private static final String CONFIGURATION_PATH = "/etc/mysql/conf.d/my.cnf";
    private static final String INIT_SCRIPT_PATH = "/docker-entrypoint-initdb.d/%s";
    private static final String WAIT_REGEX = ".*mysqld: ready for connections.*\\n";
    private static final int WAIT_TIMES = 1;
    private static final int PORT = 3306;

    private MySQLContainer() {
        this(System.getProperty(DOCKER_IMAGE_KEY, String.format("%s:%s", IMAGE, TAG)));
    }

    private MySQLContainer(final String imageWithTag) {
        super(imageWithTag);

        withExposedPorts(PORT);
        waitingFor(Wait.forLogMessage(WAIT_REGEX, WAIT_TIMES).withStartupTimeout(Duration.ofMinutes(5L)));
        withLogConsumer(outputFrame -> LOG.info(imageWithTag + " " + outputFrame.getUtf8String()));
    }

    public static MySQLContainer newInstance() {
        return new MySQLContainer();
    }

    public static MySQLContainer newInstance(final String imageWithTag) {
        return new MySQLContainer(imageWithTag);
    }

    public MySQLContainer withMySQLConfiguration(final MySQLConfiguration mySQLConfiguration) {
        if (mySQLConfiguration.getNetwork() != null) {
            withNetwork(mySQLConfiguration.getNetwork());
        }
        withEnv(extractEnvEntries(mySQLConfiguration));
        withClasspathResourceMapping(mySQLConfiguration.getConfPath(), CONFIGURATION_PATH, BindMode.READ_ONLY);

        for (String initScript : mySQLConfiguration.getInitScripts()) {
            withClasspathResourceMapping(initScript, String.format(INIT_SCRIPT_PATH, initScript), BindMode.READ_ONLY);
        }

        return self();
    }

    @Override
    public int getPort() {
        return getMappedPort(getContainerInfo().getConfig().getExposedPorts()[0].getPort());
    }

    @Override
    public void doStart() {
        super.doStart();
    }

    private Map<String, String> extractEnvEntries(final MySQLConfiguration mySQLConfiguration) {
        final Map<String, String> envValues = new HashMap<>();
        // Root password is mandatory for starting mysql docker instance
        envValues.put(ROOT_PASSWORD_KEY, mySQLConfiguration.getPassword());
        envValues.computeIfAbsent(DATABASE_KEY, val -> mySQLConfiguration.getSchema());
        envValues.computeIfAbsent(USER_KEY, val -> mySQLConfiguration.getUsername());
        envValues.computeIfAbsent(PASSWORD_KEY, val -> mySQLConfiguration.getPassword());

        return envValues;
    }
}

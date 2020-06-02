/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hadoop.example;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.ProxyAuthenticator;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

/** A wrapper that starts a test HTTP proxy server on localhost and executes {@link SampleOracleBmcHadoopJob}. */
@Slf4j
public class SampleOracleBmcHadoopWithProxy {
    /**
     * Runner for sample hadoop job.
     * <p>
     * This expects 6 arguments:
     * <ol>
     * <li>--config [path to config] The path to the configuration XML file</li>
     * <li>--namespace [bucket namespace] The bucket namespace</li>
     * <li>--bucket [bucket] The bucket to use.  This must be created beforehand</li>
     * <li>--proxy-port [port #] The port to host the proxy on localhost.  This must correlate with HTTP_PROXY_URI
     * defined in the configuration XML file</li>
     * <li>--proxy-username [username] The username to use for the proxy.  This must correlate with HTTP_PROXY_USERNAME
     * defined in the configuration XML file</li>
     * <li>--proxy-password [password] The passowrd to use for the proxy. This must correlate with HTTP_PROXY_PASSWORD
     * defined in the configuration XML file</li>
     * </ol>
     * <p>
     * This runner will create a test input file in a file '/samplehadoopjob/input.dat', and job results will be written
     * to '/samplehadoopjob/output'.
     *
     * @param args the command line arguments
     * @throws Exception if an error occurred
     */
    public static void main(final String... args) throws Exception {
        final CliOptions options = CliOptions.parseArguments(args);

        // Start a HTTP proxy server for clients to connect to locally
        final HttpProxyServer proxyServer =
                newProxyServer(
                        options.getProxyPort(),
                        options.getProxyUsername(),
                        options.getProxyPassword());

        int returnStatus = 0;
        try {
            final SampleOracleBmcHadoopJob job =
                    new SampleOracleBmcHadoopJob(
                            options.getConfigurationFile(),
                            options.getNamespace(),
                            options.getBucket());
            returnStatus = job.execute();
        } finally {
            proxyServer.stop();
        }

        System.exit(returnStatus);
    }

    /**
     * Starts and returns a new HTTP proxy server that is used as an example proxy server for clients to connect to.
     * It is hosted on 'localhost' for the given {@code port}, {@code username}, and {@code password}.
     *
     * @param port the port for the proxy to listen on
     * @param username the username to validate the client connection with
     * @param password the password to validate the client connection with
     * @return a HTTP proxy server instance
     */
    private static HttpProxyServer newProxyServer(
            final int port, final String username, final String password) {
        final ProxyAuthenticator authenticator =
                new ProxyAuthenticator() {
                    @Override
                    public boolean authenticate(final String username, final String password) {
                        return username.equals(username) && password.equals(password);
                    }

                    @Override
                    public String getRealm() {
                        return null;
                    }
                };
        final HttpProxyServer proxyServer =
                DefaultHttpProxyServer.bootstrap()
                        .withPort(port)
                        .withProxyAuthenticator(authenticator)
                        .start();

        LOG.info("Started authenticated proxy server on port [{}]", port);
        return proxyServer;
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    private final static class CliOptions {
        @Option(name = "-c", aliases = "--config", usage = "The path to the configuration XML file")
        private String configurationFile;

        @Option(name = "-n", aliases = "--namespace", usage = "The bucket namespace")
        private String namespace;

        @Option(name = "-b", aliases = "--bucket", usage = "The bucket to use")
        private String bucket;

        @Option(
            name = "-l",
            aliases = "--proxy-port",
            usage = "Specify the port for starting the proxy server on localhost"
        )
        private int proxyPort = 8000;

        @Option(
            name = "-u",
            aliases = "--proxy-username",
            usage =
                    "The username for the proxy.  This must match what is specified in the configuration file"
        )
        private String proxyUsername = "username";

        @Option(
            name = "-p",
            aliases = "--proxy-password",
            usage =
                    "The password for the proxy.  This must match what is specified in the configuration file"
        )
        private String proxyPassword = "password";

        public static CliOptions parseArguments(final String... args) {
            final CliOptions options = new CliOptions();
            final CmdLineParser parser = new CmdLineParser(options);

            try {
                if (args.length < 1) {
                    throw new CmdLineException(parser, "No arguments were provided");
                }
                parser.parseArgument(args);
            } catch (final CmdLineException ex) {
                System.err.println(ex.getMessage());
                parser.printUsage(System.err);
                System.exit(-1);
            }

            return options;
        }
    }
}

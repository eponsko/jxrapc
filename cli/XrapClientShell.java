package com.wpl.xrapc.cli;

import com.wpl.xrapc.XrapException;
import com.wpl.xrapc.XrapPeer;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static sun.misc.ThreadGroupUtils.getRootThreadGroup;

public class XrapClientShell {
    static Logger log;
    private Thread xrapClient;
    private String host;
    private int port;
    private int timeoutSeconds;
    private XrapPeer client;
    private boolean isServer = true;
    public XrapClientShell(String[] args) throws UsageException {
        parseArgs(args);
        this.port = 7777;
        this.host = "127.0.0.1";
        this.timeoutSeconds = 5;

        client = new XrapPeer(host, port, isServer);
        client.setTimeout(timeoutSeconds);
        client.addHandler(new TestResource());
    }

    private static Thread[] getAllThreads() {
        final ThreadGroup root = getRootThreadGroup();
        final ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        int nAlloc = thbean.getThreadCount();
        int n = 0;
        Thread[] threads;
        do {
            nAlloc *= 2;
            threads = new Thread[nAlloc];
            n = root.enumerate(threads, true);
        } while (n == nAlloc);
        return java.util.Arrays.copyOf(threads, n);
    }

    public static void main(String[] args) {
        log = LoggerFactory.getLogger(XrapClientShell.class);
        try {
            new XrapClientShell(args).run();
        } catch (UsageException ex) {
            log.error(ex.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("xrapc", buildCommandLineOptions(), true);
        } catch (IOException ex) {
            log.error(ex.getMessage());
        }
    }

    private static Options buildCommandLineOptions() {
        Options options = new Options();

        options.addOption(
                Option.builder("t")
                        .longOpt("timeout")
                        .desc("Request timeout in seconds")
                        .argName("<SECONDS>t")
                        .hasArg(true)
                        .type(Number.class)
                        .build());
        options.addOption(
                Option.builder("c")
                        .longOpt("client")
                        .desc("Connect rather than bind")
                        .hasArg(false)
                        .type(Boolean.class)
                        .build());
        options.addOption(
                Option.builder("d")
                        .longOpt("debug")
                        .desc("set DEBUG log level")
                        .hasArg(false)
                        .type(Boolean.class)
                        .build());

        //	log.info("options: " + options.toString());
        return options;
    }

    private void parseArgs(String[] args) throws UsageException {
        try {
            CommandLineParser parser = new DefaultParser();
            Options options = buildCommandLineOptions();
            CommandLine cmd = parser.parse(options, args);
            String[] remainingArgs = cmd.getArgs();

            for (String str : cmd.getArgList()) {
                log.info("CommandLineParser: " + str);
            }
            Number timeoutArg = (Number) cmd.getParsedOptionValue("timeout");
            if (timeoutArg != null) {
                timeoutSeconds = timeoutArg.intValue();
            }

            if (cmd.hasOption('c')) {
                log.info("Running in Client mode");
                isServer = false;
            } else {
                log.info("Running in Server mode");
            }
            if (cmd.hasOption('d')) {
                log.info("Setting DEBUG loglevel");
                // How to do this with SLF4j?
                //Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

            }
        } catch (ParseException ex) {
            throw new UsageException(ex.getMessage());
        }
    }

    private void run() throws IOException {
        xrapClient = new Thread(client);
        xrapClient.start();
        ConsoleReader reader = new ConsoleReader();
        try {
            PrintWriter out = new PrintWriter(reader.getOutput());
            if(isServer) {
                reader.setPrompt("S> ");
            } else {
                reader.setPrompt("C> ");
            }

            StringBuilder commandString = new StringBuilder();
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.endsWith("\\")) {
                    commandString.append(line.substring(0, line.length() - 1));
                    commandString.append(" ");
                    continue;
                }
                commandString.append(line);

                try {
                    if (parseLine(commandString.toString())) {
                        break;
                    }
                } catch (UsageException ex) {
                    out.println(ex.getMessage());
                } catch (XrapException ex) {
                    out.println(ex.getMessage());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                commandString = new StringBuilder();
            }
        } finally {

            log.info("Terminating XrapPeer..");
            client.terminate();
            log.info("Terminating reader..");
            reader.shutdown();
        }

    }

    private boolean parseLine(String commandString) throws UsageException, XrapException, InterruptedException {
        CommandTokenizer tok = new CommandTokenizer(commandString);

        String commandName = tok.nextToken();
        if (commandName == null) {
            return false;
        }

        if (BaseCommand.validMethodName(commandName)) {
            BaseCommand command = BaseCommand.createCommand(commandName);
            parseXrapCommand(command, tok);
        } else if (commandName.equalsIgnoreCase("quit") || commandName.equalsIgnoreCase("exit")) {
            return true;
        } else {
            throw new UsageException(String.format("Unrecognised command \"" + commandName + "\""));
        }
        return false;
    }

    private void parseXrapCommand(BaseCommand command, CommandTokenizer tok) throws UsageException, XrapException, InterruptedException {
        String resource = tok.nextToken();
        if (resource == null) {
            throw new UsageException(String.format("Must supply resource with '%s'", command.getName()));
        }
        command.setResource(resource);

        String item;
        while ((item = tok.nextToken()) != null) {
            Utils.parseItem(command, item);
        }
        command.run(client);
    }


    static class CommandTokenizer {
        char[] line;
        int pos;

        CommandTokenizer(String line) {
            this.line = line.toCharArray();
        }

        String nextToken() {
            skipWhite();
            if (pos == line.length) {
                return null;
            }

            if (line[pos] == '\'' || line[pos] == '"') {
                return matchString();
            }
            return matchWord();
        }

        private void skipWhite() {
            while (pos < line.length && Character.isWhitespace(line[pos]))
                pos++;
        }

        private String matchWord() {
            int startpos = pos;
            while (pos < line.length && !Character.isWhitespace(line[pos]))
                pos++;
            return new String(line, startpos, pos - startpos);
        }

        private String matchString() {
            char quoteChar = line[pos];
            pos++;
            int startpos = pos;
            while (pos < line.length) {
                if (line[pos] == quoteChar) {
                    String result = new String(line, startpos, pos - startpos);
                    pos++;
                    return result;
                }
                pos++;
            }
            // Unterminated string.
            return new String(line, startpos, pos - startpos);
        }
    }
}

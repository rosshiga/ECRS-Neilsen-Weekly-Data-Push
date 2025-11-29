package com.ecrs.nielsen;

import com.jcraft.jsch.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * SFTP client for uploading files to the Nielsen server.
 */
public class SftpUploader {

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    /**
     * Creates a new SFTP uploader.
     *
     * @param host     The SFTP host
     * @param port     The SFTP port
     * @param username The SFTP username
     * @param password The SFTP password
     */
    public SftpUploader(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /**
     * Uploads a file to the SFTP server.
     *
     * @param localFile  The local file to upload
     * @param remotePath The remote path (including filename)
     * @throws IOException if the upload fails
     */
    public void upload(File localFile, String remotePath) throws IOException {
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            JSch jsch = new JSch();

            // Create session
            session = jsch.getSession(username, host, port);
            session.setPassword(password);

            // Configure session to skip host key checking
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            // Set timeout
            session.setTimeout(30000);

            System.out.println("  Connecting to " + host + ":" + port + "...");
            session.connect();
            System.out.println("  Connected.");

            // Open SFTP channel
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();
            System.out.println("  SFTP channel opened.");

            // Ensure remote directory exists
            String remoteDir = remotePath.substring(0, remotePath.lastIndexOf('/'));
            try {
                channelSftp.cd(remoteDir);
            } catch (SftpException e) {
                // Directory doesn't exist, try to create it
                System.out.println("  Creating remote directory: " + remoteDir);
                mkdirs(channelSftp, remoteDir);
                channelSftp.cd(remoteDir);
            }

            // Upload file
            String remoteFileName = remotePath.substring(remotePath.lastIndexOf('/') + 1);
            System.out.println("  Uploading " + localFile.getName() + " to " + remotePath + "...");

            try (FileInputStream fis = new FileInputStream(localFile)) {
                channelSftp.put(fis, remoteFileName, ChannelSftp.OVERWRITE);
            }

            System.out.println("  File uploaded successfully.");

        } catch (JSchException | SftpException e) {
            throw new IOException("SFTP upload failed: " + e.getMessage(), e);
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        }
    }

    /**
     * Creates remote directories recursively.
     */
    private void mkdirs(ChannelSftp channelSftp, String path) throws SftpException {
        String[] folders = path.split("/");
        StringBuilder currentPath = new StringBuilder();

        for (String folder : folders) {
            if (folder.isEmpty()) {
                currentPath.append("/");
                continue;
            }

            currentPath.append(folder).append("/");
            try {
                channelSftp.cd(currentPath.toString());
            } catch (SftpException e) {
                channelSftp.mkdir(currentPath.toString());
            }
        }
    }
}


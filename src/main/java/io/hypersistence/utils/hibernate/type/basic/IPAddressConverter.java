package io.hypersistence.utils.hibernate.type.basic;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPAddressConverter {

    public static Integer convertIPToInteger(String ipAddress) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            byte[] bytes = inetAddress.getAddress();

            int result = 0;
            for (byte b : bytes) {
                result = (result << 8) | (b & 0xFF);
            }

            return result;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static String convertIntToIPAddress(int ipAddressInt) {
        try {
            byte[] ipAddressBytes = new byte[4];
            ipAddressBytes[0] = (byte) ((ipAddressInt >> 24) & 0xFF);
            ipAddressBytes[1] = (byte) ((ipAddressInt >> 16) & 0xFF);
            ipAddressBytes[2] = (byte) ((ipAddressInt >> 8) & 0xFF);
            ipAddressBytes[3] = (byte) (ipAddressInt & 0xFF);

            InetAddress inetAddress = InetAddress.getByAddress(ipAddressBytes);
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }
}

package suppliers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Checksum {
    private Checksum() { }

    /**
     * 16-битная контрольная сумма файла (как в Internet checksum, RFC 1071): байты читаются через FileChannel и
     * ByteBuffer, склеиваются в 16-битные слова (big-endian), суммируются с циклическим переносом, результат инвертируется.
     * Нечётный последний байт дополняется нулём справа.
     */
    public static int checksum16(Path file) throws IOException {
        long sum = 0;
        int high = 0;
        boolean haveHigh = false;
        ByteBuffer buf = ByteBuffer.allocate(8192);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            while (ch.read(buf) != -1) {
                buf.flip();
                while (buf.hasRemaining()) {
                    int b = buf.get() & 0xFF;
                    if (!haveHigh) {
                        high = b;
                        haveHigh = true;
                    } else {
                        sum += (high << 8) | b;
                        sum = (sum & 0xFFFF) + (sum >>> 16);
                        haveHigh = false;
                    }
                }
                buf.clear();
            }
        }
        if (haveHigh) sum += high << 8;
        while ((sum >>> 16) != 0) sum = (sum & 0xFFFF) + (sum >>> 16);
        return (int) (~sum & 0xFFFF);
    }

    /** SHA-256 файла в виде hex-строки в верхнем регистре. */
    public static String sha256(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                md.update(buf, 0, n);
            }
            StringBuilder sb = new StringBuilder();
            for (byte b : md.digest()) {
                sb.append(String.format("%02X", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

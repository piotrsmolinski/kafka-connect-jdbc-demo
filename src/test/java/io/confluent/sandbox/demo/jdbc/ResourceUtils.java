package io.confluent.sandbox.demo.jdbc;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ResourceUtils {

  public static byte[] readFile(String path) {
    File file = new File(path);
    if (!file.exists()) {
      throw new RuntimeException("File not found: " + path);
    }
    try (InputStream stream = new FileInputStream(file)) {
      return readStream(stream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] readResource(String path) {
    try (InputStream stream = ClassLoader.getSystemResourceAsStream(path)) {
      if (stream == null) {
        throw new RuntimeException("Resource not found: " + path);
      }
      return readStream(stream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void copyStream(InputStream inputStream, OutputStream outputStream) {
    try {
      byte[] buffer = new byte[8192];
      for (; ; ) {
        int r = inputStream.read(buffer);
        if (r < 0) break;
        outputStream.write(buffer, 0, r);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] readStream(InputStream inputStream) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    copyStream(inputStream, baos);
    return baos.toByteArray();
  }

  public static void createDir(String dir) {
    try {
      Files.createDirectories(FileSystems.getDefault().getPath(dir));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteDir(String dir) {
    if (!Files.isDirectory(FileSystems.getDefault().getPath(dir))) return;
    try {
      Files.walk(FileSystems.getDefault().getPath(dir))
              .sorted(Comparator.reverseOrder())
              .forEach(path-> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void clearDir(String dir) throws Exception {
    Files.list(FileSystems.getDefault().getPath(dir))
            .map(Path::toFile)
            .forEach(File::delete);
  }

  public static void copyFile(String source, String target) throws Exception {
    try (InputStream istream = new FileInputStream(source);
         OutputStream ostream = new FileOutputStream(target)) {
      copyStream(istream, ostream);
    }
  }

  public static List<String> readLines(String path) throws Exception {
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      for (;;) {
        String line = reader.readLine();
        if (line==null) break;
        lines.add(line);
      }
    }
    return lines;
  }

  public static void writeLines(String path, List<String> lines) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileWriter(path))) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

}

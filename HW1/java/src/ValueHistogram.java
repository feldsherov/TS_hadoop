import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class ValueHistogram {


    public static void main(String[] args) throws Exception {
        Path path = FileSystems.getDefault().getPath("/Users/feldsherov/techsphere/hadoop/HW1/java/src/data.short.txt");
        String in = new String(Files.readAllBytes(path));
        String[] st = in.split("\\r?\\n");
        System.out.println(st.length);
        for (String a: st) {
            if (a.startsWith("\"")) {
                continue;
            }
            String[] parts = a.split(",");
            System.out.printf("%d %s\n", new Integer(parts[1]), parts[4]);
        }
    }
}

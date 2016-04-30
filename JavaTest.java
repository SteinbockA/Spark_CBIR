import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;

/**
 * Created by aaron on 16-4-21.
 */
public class JavaTest {
    public static void main(String[] args) throws Exception{
        BufferedImage logo = ImageIO.read(new File("/home/aaron/图片/CBIR/source/img/test4.png"));
        System.out.println(logo.getClass());
    }
}

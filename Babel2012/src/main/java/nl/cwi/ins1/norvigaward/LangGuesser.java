package nl.cwi.ins1.norvigaward;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;

public class LangGuesser extends EvalFunc<String> {
	static {
		File tempdir;
		try {
			// This is ugly, but the langdetect library insists on a folder with
			// its profiles, so we load a ZIP file containing them from the
			// classpath, extract to a temporary directory, and have them loaded
			// from there. Yes, to make a temporary directory we first create a
			// temporary file, then delete it, and then abuse its name for the
			// new directory. Ugly again, but hopefully effective.
			tempdir = File.createTempFile("langdetect-profiles-",
					Long.toString(System.nanoTime()));
			tempdir.delete();
			tempdir.mkdir();

			InputStream zipfile = LangGuesser.class
					.getResourceAsStream("profiles.zip");
			unZip(zipfile, tempdir);

			DetectorFactory.loadProfile(tempdir);
			deleteFolder(tempdir);
			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		//
	}

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			String str = (String) input.get(0);
			Detector detector = DetectorFactory.create();
			detector.append(str);
			return detector.detect();

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}

	public static void unZip(InputStream zippedIS, File outputFolder)
			throws IOException {
		byte[] buffer = new byte[1024];

		ZipInputStream zis = new ZipInputStream(zippedIS);
		ZipEntry ze = zis.getNextEntry();

		while (ze != null) {
			String fileName = ze.getName();
			File newFile = new File(outputFolder + File.separator + fileName);
			new File(newFile.getParent()).mkdirs();
			FileOutputStream fos = new FileOutputStream(newFile);
			int len;
			while ((len = zis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
			ze = zis.getNextEntry();
		}

		zis.closeEntry();
		zis.close();
	}
	
	public static void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) { //some JVMs return null for empty dirs
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	    folder.delete();
	}

}

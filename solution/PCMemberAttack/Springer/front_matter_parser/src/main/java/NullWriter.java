import java.io.IOException;
import java.io.Writer;

/**Writes to nowhere*/
public class NullWriter extends Writer {

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    //Do nothing
  }

  @Override
  public void flush() throws IOException {
    //Do nothing
    
  }

  @Override
  public void close() throws IOException {
    //Do nothing
  } 
}
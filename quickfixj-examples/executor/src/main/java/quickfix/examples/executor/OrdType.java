package quickfix.examples.executor;
import quickfix.StringField;


public class OrdType extends StringField
{
  public OrdType()
  {
    super(40);
  }

  public OrdType(String data)
  {
     super(40, data);
  }


}

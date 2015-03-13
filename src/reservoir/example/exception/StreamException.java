package reservoir.example.exception;

public class StreamException extends Exception {

    private static final long serialVersionUID = 2799454558694307191L;
    
    public StreamException(){
        super();
    }
    
    public StreamException(String msg){
        super(msg);
    }

}

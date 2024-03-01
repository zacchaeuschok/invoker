package runtime.taskcore;

public interface StateManager {
    void write(String key, String value);

    String read(String key);
}

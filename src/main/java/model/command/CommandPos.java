/*
 *@Type CommandPos.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:35
 * @version
 */
package model.command;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
public class CommandPos implements Serializable {
    private int pos;
    private int len;
    private String filePath;

    public CommandPos(int pos, int len) {
        this.pos = pos;
        this.len = len;
        this.filePath = null;
    }

    public CommandPos(int pos, int len,String filePath) {
        this.pos = pos;
        this.len = len;
        this.filePath = filePath;
    }

    @Override
    public String toString() {
        return "CommandPos{" +
                "pos=" + pos +
                ", len=" + len +
                ", filePath=" + filePath +
                '}';
    }
}

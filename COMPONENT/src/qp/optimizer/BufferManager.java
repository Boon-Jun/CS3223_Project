/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        buffPerJoin = numJoin==0? numBuffer : (numBuffer / numJoin);
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

    public static int getBuffers() {
        return numBuffer;
    }


}

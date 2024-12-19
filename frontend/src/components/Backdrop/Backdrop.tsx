import styles from './backdrop.module.scss';
import Store from '../../store/Store.ts';
import { observer } from 'mobx-react-lite';
import Greeting from '../Greeting/Greeting.tsx';
import Autorization from '../Autorization/Autorization.tsx';
import Optimization from '../Optimization/Optimization.tsx';
import Result from "../Result/Result.tsx";
import ClientOptimization from "../ClientOptimization/ClientOptimization.tsx";

const Backdrop = observer(() => {
    return (
        <div className={styles.backdrop}>
            {Store.currentPage === 'greeting' && <Greeting />}
            {Store.currentPage === 'chooseCurrency' && <Autorization />}
            {Store.currentPage === 'freeOptimization' && <Optimization type={'free'} />}
            {Store.currentPage === 'paidOptimization' && <Optimization type={'paid'} />}
            {Store.currentPage === 'result' && <Result />}
            {Store.currentPage === 'clientOptimization' && <ClientOptimization />}
        </div>
    );
});

export default Backdrop;
